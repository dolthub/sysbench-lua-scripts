require("types_common")

function prepare()
    sysbench.opt.threads = 1
    drv = sysbench.sql.driver()
    con = drv:connect()
    create_types_table(drv, con, 1)
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local id = sysbench.rand.default(1, 10000)
    con:query(string.format("DELETE FROM sbtest1 WHERE id = %d", id))
    local row_values = row_for_id(id)
    con:query(string.format("INSERT INTO sbtest1 VALUES %s", row_values))
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    print("Dropping table 'sbtest1'")
    con:query("DROP TABLE IF EXISTS sbtest1")
end

