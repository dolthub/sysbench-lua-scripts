if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepare, warmup, run, cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 100000},
    create_table_options = {"Extra CREATE TABLE options", ""},
}

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   print("Creating table 'sbtest1'")

   local create_query = string.format( [[
CREATE TABLE sbtest1 (
	a INT NOT NULL,
  b INT NOT NULL,
	PRIMARY KEY(a)
); ]] .. sysbench.opt.create_table_options)

   con:query(create_query)

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into 'sbtest1'", sysbench.opt.table_size))
   end

   local query = [[INSERT INTO sbtest1 (a,b) VALUES]]

   local str_vals = {"val0", "val1", "val2"}
   math.randomseed(0)

   con:bulk_insert_init(query)
   for i = 1, sysbench.opt.table_size do
      local row_values = "(" ..
         math.random(-2147483648, 2147483647) .. "," .. -- a
         math.random(-2147483648, 2147483647) .. ")"    -- b

      con:bulk_insert_next(row_values)
   end
   con:bulk_insert_done()

end


-- Implement parallel prepare and warmup commands, define 'prewarm' as an alias
-- for 'warmup'
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
}

local t = sysbench.sql.type

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()
end

function thread_done()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   print("Dropping table 'sbtest1'")
   con:query("DROP TABLE IF EXISTS sbtest1")
end

dolt_prepare = prepare

function prepare()
    sysbench.opt.threads = 1
    dolt_prepare()
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT * FROM sbtest1')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end
