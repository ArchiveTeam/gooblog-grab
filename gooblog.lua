local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local discovered_items_count = {
  ["all"] = 0,
  ["no?"] = 0
}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

local item_patterns = {
  ["^https?://blog%.goo%.ne%.jp/([^/]+/e/[0-9a-f]+)"]="e",
  ["^https?://blog%.goo%.ne%.jp/([^/]+/cmt/[0-9a-f]+)"]="e",
  ["^https?://blog%.goo%.ne%.jp/([^/]+/c/[0-9a-f]+)"]="c",
  ["^https?://blog%.goo%.ne%.jp/([^/]+/arcv/.*[%?&;]c=[0-9a-f]+)"]="c",
  ["^https?://blog%.goo%.ne%.jp/([^/]+/m/[0-9][0-9][0-9][0-9][0-9][0-9])"]="m",
  ["^https?://blog%.goo%.ne%.jp/([^/]+/d/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])"]="d",
  ["^https?://blog%.goo%.ne%.jp/photo/([0-9]+)"]="photo",
  ["^https?://blog%.goo%.ne%.jp/([0-9a-z%-_]+)"]="blog",
  ["^https?://(blogimg%.goo%.ne%.jp/.+)"]="asset",
  ["^https?://(img[^/]+goo%.ne%.jp/.+)"]="asset",
  ["^https?://([^/]*xgoo%.jp/.+)"]="asset",
  ["^https?://(blog%.goo%.ne%.jp/img/.)"]="asset",
  ["^https?://blog%.goo%.ne%.jp/portal/tags/([^/%?&;#]+)"]="tag"
}

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    if string.match(item, "%z") then
      error("Should not find NULL byte.")
    end
    if target == discovered_items then
      discovered_items_count["all"] = discovered_items_count["all"] + 1
      if not string.match(item, "%?") then
        discovered_items_count["no?"] = discovered_items_count["no?"] + 1
      end
    end
--print("discovered", item, cjson.encode(discovered_items_count))
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_patterns) do
    value = string.match(url, pattern .. "$")
    type_ = name
    if value and not string.match(value, "/arcv/?%?")
      and not string.match(value, "/cmt/[0-9a-f]+$") then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    if found["type"] == "e" or found["type"] == "c" or found["type"] == "m"
      or found["type"] == "d" then
      found["value"] = string.gsub(found["value"], "/[ecmd]/", ":")
      newcontext["blog"], newcontext["id"] = string.match(found["value"], "^([^:]+):([^:]+)$")
    end
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      if context["id"] then
        ids[string.lower(context["id"])] = true
      end
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  if string.match(url, "^https?://twitter%.com/intent")
    or string.match(url, "^https?://www%.facebook%.com/share")
    or string.match(url, "^https?://b%.hatena%.ne%.jp/entry")
    or string.match(url, "^https?://line%.me/R/")
    or string.match(url, "^https?://mixi%.jp/share")
    or string.match(url, "^https?://blog%.goo%.ne%.jp/portal/webpush/setting")
    or (
      item_type == "m"
      and string.match(url, "^https?://blog%.goo%.ne%.jp/[^/]+/m/[0-9]+/?[0-9]*%?st=[01]$")
    ) then
    return false
  end

  if item_type == "e"
    and string.match(url, "[%?&;]st=[01]")
    and not context["2001"] then
    return false
  end

  local skip = false
  for pattern, type_ in pairs(item_patterns) do
    match = string.match(url, pattern)
    local found = false
    if match then
      if type_ == "m" or type_ == "d" then
        local n = tonumber(string.match(match, "([0-9]+)$"))
        if (type_ == "m" and (n > 200400 or n < 202511))
          or (type_ == "d" and (n > 20040000 or n < 20251100)) then
          found = true
        else
          return false
        end
      else
        found = true
      end
    end
    if found then
      if type_ == "e" or type_ == "c" or type_ == "m" or type_ == "d" then
        if type_ == "c" and string.match(match, "/arcv/?%?") then
          match = string.match(match, "([^/]+)") .. ":" .. string.match(match, "[%?&;]c=([0-9a-f]+)")
        else
          match = string.gsub(match, "/[ecmd]m?t?/([0-9a-f]+)$", ":%1")
        end
      end
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name
        and not ids[string.lower(string.match(match, "([^:]+)$"))] then
        discover_item(discovered_items, new_item)
        if type_ ~= "blog" then
          skip = true
        end
      end
    end
  end
  if skip then
    return false
  end

  if not string.match(url, "^https?://[^/]*goo%.ne%.jp/")
    and not string.match(url, "^https?://[^/]*xgoo%.jp/") then
    discover_item(discovered_outlinks, string.match(percent_encode_url(url), "^([^%s]+)"))
    return false
  end

  for _, pattern in pairs({
    "([0-9a-zA-Z_%-%%]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end]]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    local post_body = nil
    local post_url = nil
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if item_type == "m"
      and string.match(url_, "%?") then
      check(string.match(url_, "^([^%?]+)"))
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local headers = {}
      table.insert(urls, {
        url=url_,
        headers=headers
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "[=&]") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. ")=?[^%?&;]*", "%1" .. value)
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  if allowed(url)
    and status_code < 300
    and item_type ~= "asset" then
    html = read_file(file)
    if item_type ~= "photo" and item_type ~= "m" and item_type ~= "d" then
      local page_num = string.match(url, "[%?&;]page=([0-9]+)")
        or string.match(url, "[%?&;]p=([0-9]+)")
        or string.match(url, "/([0-9]+)$")
      if page_num and tonumber(page_num) > 100000 then
        error("Too high page number.")
      end
    end
    if item_type == "e" then
      for comment_name in string.gmatch(html, "<span class=\"comment%-name\">%s*([0-9a-zA-Z%-_]+)") do
        discover_item(discovered_items, "commentname:" .. comment_name)
      end
      if string.match(url, "^https?://blog%.goo%.ne%.jp/[^/]+/e/[0-9a-f]+$") then
        context["2001"] = string.match(html, "%(2001%)")
      end
    end
    local counts = nil
    if string.match(string.match(url, "^([^%?]+)"), "/follower") then
      counts = {}
      for a, b in pairs(discovered_items_count) do
        counts[a] = b
      end
    end
    if item_type == "blog" then
      check("https://blog.goo.ne.jp/" .. item_value .. "/follower")
      check("https://blog.goo.ne.jp/" .. item_value .. "/images")
      check("https://blog.goo.ne.jp/" .. item_value .. "?fm=follower")
    end
    if item_type == "c" then
      check("https://blog.goo.ne.jp/" .. context["blog"] .. "/arcv/?c=" .. context["id"])
    end
    if item_type == "photo" then
      check("https://blog.goo.ne.jp/api/photo_channel/get_photos?chno=" .. item_value .. "&p=0")
      for _, path in pairs({
        "/photo/" .. item_value .. "/play?type=",
        "/swf/player/" .. item_value .. "?type=",
      }) do
        if string.match(path, "[%?&;]type=$") then
          for _, type_string in pairs({"", "s", "l", "m"}) do
            check(urlparse.absolute(url, path .. type_string))
          end
          check(urlparse.absolute(url, string.match(path, "^(.+)[%?&;]type=$")))
        end
      end
    end
    if string.match(url, "/api/photo_channel/get_photos%?") then
      json = cjson.decode(html)
      if string.match(url, "[%?&;]p=")
        and (get_count(json["photos"]) > 1 or json["count"] > 0) then
        check(increment_param(url, "p", 0, 1))
      end
      if json["blog_id"] then
        ids[json["blog_id"] ] = true
        check("https://blog.goo.ne.jp/api/photo_channel_smile/get_channel_smile_count?bid=" .. json["blog_id"] .. "&chno=" .. item_value)
        for _, data in pairs(json["photos"]) do
          ids[data["photo_id"] ] = true
          check("https://blog.goo.ne.jp/api/photo_channel_smile/get_photo_smile_count?bid=" .. json["blog_id"] .. "&pid=" .. data["photo_id"])
        end
      end
    end
    if string.match(url, "/photo/[0-9]+/pn/[0-9a-f]+$") then
      local photo_id = string.match(url, "([0-9a-f]+)$")
      ids[photo_id] = true
      check("https://blog.goo.ne.jp/api/photo_channel/get_photos?chno=" .. item_value .. "&photo_id=" .. photo_id)
    end
    if string.match(url, "/arcv/?%?")
      and (
        string.match(html, "<div class=\"entry%-body\">%s*<div class=\"entry%-body%-text\">%s*<aside>該当する記事はありません。<br%s*/>")
        or string.match(html, "<div class=\"entry%-body\">%s*<div class=\"entry%-body%-text\">%s*該当する記事はありません。<br%s*/>")
      ) then
      return urls
    end
    for newurl in string.gmatch(string.gsub(html, "&[qQ][uU][oO][tT];", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end

    if counts then
      local equal = counts["no?"] == discovered_items_count["no?"]
      if equal then
        return {}
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200
    and http_stat["statcode"] ~= 404 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 11
    if status_code == 301
      or status_code == 401
      or status_code == 410 then
      tries = maxtries + 1
    end
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["gooblog-zpcs3o24hlm1vkcn"] = discovered_items,
    ["urls-nqedu6rncbhsloax"] = discovered_outlinks
  }) do
    print("queuing for", string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 1000 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


