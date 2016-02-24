pathtest = string.match(test, "(.*/)") or ""

dofile(pathtest .. "common.lua")

function thread_init(thread_id)
   set_vars()
end

function foo(thread_id) 
   local file

   file = io.open('/root/david/'..thread_id..'/file', "r")
   io.input(file)
   while true do
      local line = io.read()
      if (line == nil) then break end
      rs = db_query(line)
   end
   if unexpected_condition then error() end
end

function mydb_connect()
	db_connect()
	if unexpected_condition then error() end
end

function event(thread_id)
    local i=0
    while i==0 do
        local pfile = io.popen('ls -1 /root/david/'..thread_id..'/file')
        for filename in pfile:lines() do
            i=i+1
        end
        pfile:close()
    end
    local status,err = pcall(foo,thread_id)
    os.execute('rm -f /root/david/'..thread_id..'/file')
    if not status then
        db_disconnect()
        os.execute('sleep 1')
        local st,e= pcall(mydb_connect)
        while not st do
            st,e= pcall(mydb_connect)
            os.execute('sleep 0.5');
       end 
    end
end


