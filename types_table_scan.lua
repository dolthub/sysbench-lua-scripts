require('types_common')

function prepare()
    sysbench.opt.threads = 1
    drv = sysbench.sql.driver()
    con = drv:connect()
    create_types_table(drv, con)
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT * FROM sbtest1 WHERE small_int_col > 0')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    print("Dropping table 'sbtest1'")
    con:query("DROP TABLE IF EXISTS sbtest1")
end

