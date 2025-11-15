# encoding=utf8
import datetime
from distutils.version import StrictVersion
import functools
import hashlib
import os
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string
import re

if sys.version_info[0] < 3:
    from urllib import unquote
else:
    from urllib.parse import unquote

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

from tornado import httpclient

import requests
import zstandard

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

class HigherVersion:
    def __init__(self, expression, min_version):
        self._expression = re.compile(expression)
        self._min_version = min_version

    def search(self, text):
        for result in self._expression.findall(text):
            if result >= self._min_version:
                print('Found version {}.'.format(result))
                return True

WGET_AT = find_executable(
    'Wget+AT',
    HigherVersion(
        r'(GNU Wget 1\.[0-9]{2}\.[0-9]{1}-at\.[0-9]{8}\.[0-9]{2})[^0-9a-zA-Z\.-_]',
        'GNU Wget 1.21.3-at.20241119.01'
    ),
    [
        './wget-at',
        '/home/warrior/data/wget-at'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20251115.03'
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c1}.0.{c2}.{c3} Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c1}.0.{c2}.{c3} Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c1}.0.{c2}.{c3} Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c1}.0.{c2}.{c3} Safari/537.36 Edg/{c1}.0.{e2}.{e3}',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c1}.0.{c2}.{c3} Safari/537.36 Edg/{c1}.0.{e2}.{e3}',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{c1}.0.{c2}.{c3} Safari/537.36 Edg/{c1}.0.{e2}.{e3}',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:{c1}.0) Gecko/20100101 Firefox/{c1}.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:{c1}.0) Gecko/20100101 Firefox/{c1}.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:{c1}.0) Gecko/20100101 Firefox/{c1}.0',
    'python-requests/2.32.5',
    'Wget/1.21.4',
    'curl/8.17.0',
    'GoogleBot',
    '',
]
EXTRA_WGET_ARGS = [
    ('--secure-protocol', 'PFS'),
    ('--secure-protocol', 'TLSv1_1'),
    ('--secure-protocol', 'TLSv1_2'),
    ()
]
BANNED_LIST = []
with open('words_alpha.txt', 'r') as f:
    WORDS = [s.strip() for s in f]
TRACKER_ID = 'gooblog'
TRACKER_ID_ORIGINAL = TRACKER_ID
TRACKER_HOST = 'legacy-api.arpa.li'
MULTI_ITEM_SIZE = 100


def check_user_agent(user_agent):
    print('Trying user-agent', user_agent)
    tries = 0
    while tries < 20:
        returned = subprocess.run(
            [
                WGET_AT,
                '-U', user_agent,
                '--host-lookups', 'dns',
                '--hosts-file', '/dev/null',
                '--resolvconf-file', '/dev/null',
                '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
                '--output-document', '-',
                '--max-redirect', '0',
                '--timeout', '5',
                '--save-headers',
                '--no-check-certificate',
                '--header', 'Accept-Language: ja-JP,ja;q=0.9',
                'https://blog.goo.ne.jp/staffblog/e/0fa1124a1c46191c546de90826498b46'
            ],
            timeout=60,
            capture_output=True
        )
        if returned.stdout.startswith(b'HTTP/1.1 200 OK'):
            return True
        elif b'ERROR 403' in returned.stderr:
            return False
        tries += 1
        time.sleep(min(1.5**tries, 600))
    return False


def make_random_user_agent():
    return ''.join(random.choices(
        string.digits+string.ascii_letters+' ;/.-_()',
        k=random.randint(12, 40)
    ))


def make_dictionary_user_agent():
    string = ''
    used_bracket = False
    chosen = None
    for _ in range(random.randint(3, 9)):
        if chosen is not None:
            string += chosen
            if chosen == ') ':
                used_bracket = False
            elif chosen == ' (':
                assert not used_bracket
                used_bracket = True
        string += random.choice(WORDS)
        concat = list(zip(*{
            ' ': 1,
            '; ': 0.2,
            '/': 0.3,
            '.': 0.1,
            (' (' if not used_bracket else ') '): 0.3,
            '-': 0.3
        }.items()))
        chosen = random.choices(concat[0], weights=concat[1], k=1)[0]
    if used_bracket:
        string += ')'
    if random.random() < 0.7:
        string = string.title()
    return string


def make_user_agent_attempts(func, tries=10):
    for _ in range(tries):
        user_agent = func()
        if check_user_agent(user_agent):
            return user_agent


def make_list_user_agent():
    random.shuffle(USER_AGENTS)
    for template in USER_AGENTS:
        if template in BANNED_LIST:
            continue
        user_agent = template.format(
            c1=random.randint(125, 143),
            c2=random.randint(6420, 7500),
            c3=random.randint(0, 200),
            e2=random.randint(2530, 3665),
            e3=random.randint(0, 200)
        )
        if not check_user_agent(user_agent):
            BANNED_LIST.append(template)
            continue
        return user_agent


def make_user_agent():
    for func in (
        make_list_user_agent,
        functools.partial(make_user_agent_attempts, make_dictionary_user_agent),
        functools.partial(make_user_agent_attempts, make_random_user_agent)
    ):
        user_agent = func()
        if user_agent:
            return user_agent


if make_user_agent() is None:
    print('Switching to project gooblogassets.')
    TRACKER_ID = 'gooblogassets'


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            #ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 5:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')

        #user_agent = make_user_agent()
        #if user_agent is None:
        #    item.log_output('Unable to find a working user-agent.')
        #    raise Exception('Unable to find a working user-agent.')

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        item_name_hash = hashlib.sha1(item_name.encode('utf8')).hexdigest()
        escaped_item_name = item_name_hash
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            item_name_hash,
            time.strftime('%Y%m%d-%H%M%S')
        ])

        open('%(item_dir)s/%(warc_file_base)s.warc.zst' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_data.txt' % item, 'w').close()

class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.zst' % item,
              '%(data_dir)s/%(warc_file_base)s.%(dict_project)s.%(dict_id)s.warc.zst' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s_data.txt' % item,
              '%(data_dir)s/%(warc_file_base)s_data.txt' % item)

        shutil.rmtree('%(item_dir)s' % item)


def normalize_string(s):
    while True:
        temp = unquote(s).strip().lower()
        if temp == s:
            break
        s = temp
    return s


class SetBadUrls(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'SetBadUrls')

    def process(self, item):
        item['item_name_original'] = item['item_name']
        items = item['item_name'].split('\0')
        items_lower = [normalize_string(s) for s in items]
        with open('%(item_dir)s/%(warc_file_base)s_bad-items.txt' % item, 'r') as f:
            for s in {
                normalize_string(s) for s in f
            }:
                index = items_lower.index(s)
                item.log_output('Item {} is aborted.'.format(s))
                items.pop(index)
                items_lower.pop(index)
        item['item_name'] = '\0'.join(items)


class MaybeSendDoneToTracker(SendDoneToTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0:
            return self.complete_item(item)
        return super(MaybeSendDoneToTracker, self).enqueue(item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'gooblog.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class ZstdDict(object):
    created = 0
    data = None

    @classmethod
    def get_dict(cls):
        if cls.data is not None and time.time() - cls.created < 1800:
            return cls.data
        response = requests.get(
            'https://legacy-api.arpa.li/dictionary',
            params={
                'project': TRACKER_ID_ORIGINAL
            }
        )
        response.raise_for_status()
        response = response.json()
        if cls.data is not None and response['id'] == cls.data['id']:
            cls.created = time.time()
            return cls.data
        print('Downloading latest dictionary.')
        response_dict = requests.get(response['url'])
        response_dict.raise_for_status()
        raw_data = response_dict.content
        if hashlib.sha256(raw_data).hexdigest() != response['sha256']:
            raise ValueError('Hash of downloaded dictionary does not match.')
        if raw_data[:4] == b'\x28\xB5\x2F\xFD':
            raw_data = zstandard.ZstdDecompressor().decompress(raw_data)
        cls.data = {
            'id': response['id'],
            'dict': raw_data
        }
        cls.created = time.time()
        return cls.data


class WgetArgs(object):
    def realize(self, item):
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0'
        if TRACKER_ID == 'gooblog':
            user_agent = make_user_agent()
            item.log_output('Using user-agent {}.'.format(user_agent))
        wget_args = [
            WGET_AT,
            '-U', user_agent,
            '-nv',
            '--no-cookies',
            '--host-lookups', 'dns',
            '--hosts-file', '/dev/null',
            '--resolvconf-file', '/dev/null',
            '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
            '--reject-reserved-subnets',
            #'--prefer-family', ('IPv4' if 'PREFER_IPV4' in os.environ else 'IPv6'),
            '--content-on-error',
            '--lua-script', 'gooblog.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--recursive', '--level=inf',
            '--no-parent',
            '--page-requisites',
            '--timeout', '30',
            '--connect-timeout', '1',
            '--tries', 'inf',
            '--domains', 'blog.goo.ne.jp,blogimg.goo.ne.jp,i.xgoo.jp,img.news.goo.ne.jp',
            '--span-hosts',
            '--waitretry', '30',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'x-wget-at-project-version: ' + VERSION,
            '--warc-header', 'x-wget-at-project-name: ' + TRACKER_ID,
            '--warc-dedup-url-agnostic',
            '--warc-compression-use-zstd',
            '--warc-zstd-dict-no-include',
            '--header', 'Accept-Language: ja-JP,ja;q=0.9'
        ]
        dict_data = ZstdDict.get_dict()
        with open(os.path.join(item['item_dir'], 'zstdict'), 'wb') as f:
            f.write(dict_data['dict'])
        item['dict_id'] = dict_data['id']
        item['dict_project'] = TRACKER_ID_ORIGINAL
        wget_args.extend([
            '--warc-zstd-dict', ItemInterpolation('%(item_dir)s/zstdict'),
        ])

        #if TRACKER_ID == 'gooblog':
        #    wget_args.extend(random.choice(EXTRA_WGET_ARGS))

        if '--concurrent' in sys.argv:
            concurrency = int(sys.argv[sys.argv.index('--concurrent')+1])
        else:
            concurrency = os.getenv('CONCURRENT_ITEMS')
            if concurrency is None:
                concurrency = 2
        item['concurrency'] = str(concurrency)

        for item_name in item['item_name'].split('\0'):
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            wget_args.append('item-name://'+item_name)
            item_type, item_value = item_name.split(':', 1)
            if item_type == 'tag':
                wget_args.extend(['--warc-header', 'gooblog-tag: '+item_value])
                wget_args.append('https://blog.goo.ne.jp/portal/tags/'+item_value)
            elif item_type == 'blog':
                wget_args.extend(['--warc-header', 'gooblog-blog: '+item_value])
                wget_args.append('https://blog.goo.ne.jp/{}'.format(item_value))
            elif item_type == 'photo':
                wget_args.extend(['--warc-header', 'gooblog-photo: '+item_value])
                wget_args.append('https://blog.goo.ne.jp/photo/{}'.format(item_value))
            elif item_type in ('m', 'd', 'e', 'c'):
                user, value = item_value.split(':')
                wget_args.extend(['--warc-header', 'gooblog-{}-{}: {}'.format(
                    {'m': 'month', 'd': 'day', 'c': 'category', 'e': 'entry'}[item_type],
                    user, value
                )])
                wget_args.append('https://blog.goo.ne.jp/{}/{}/{}'.format(user, item_type, value))
            elif item_type == 'asset':
                url = 'https://' + item_value
                wget_args.extend(['--warc-header', 'gooblog-asset: '+url])
                wget_args.append(url)
            else:
                raise Exception('Unknown item')

        item['item_name_newline'] = item['item_name'].replace('\0', '\n')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title=TRACKER_ID,
    project_html='''
        <img class="project-logo" alt="Project logo" src="https://wiki.archiveteam.org/images/thumb/f/f3/Archive_team.png/235px-Archive_team.png" height="50px" title=""/>
        <h2>gooブログ <span class="links"><a href="https://blog.goo.ne.jp/">Website</a> &middot; <a href="https://tracker.archiveteam.org/gooblog/">Leaderboard</a> &middot; <a href="https://wiki.archiveteam.org/index.php/gooブログ">Wiki</a></span></h2>
        <p>Archiving gooブログ.</p>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker('https://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION),
    PrepareDirectories(warc_prefix=TRACKER_ID),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'item_names': ItemValue('item_name_newline'),
            'warc_file_base': ItemValue('warc_file_base'),
            'concurrency': ItemValue('concurrency')
        }
    ),
    SetBadUrls(),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.zst')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='20',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        UploadWithTracker(
            'https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.%(dict_project)s.%(dict_id)s.warc.zst'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
