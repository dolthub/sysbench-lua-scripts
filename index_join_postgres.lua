require("dolt_common_postgres")

dolt_prepare = prepare

function prepare()
    sysbench.opt.threads = 1
    dolt_prepare()
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('select a.id, a.small_int_col, b.id, b.int_col from sbtest1 a, sbtest2 b where a.id = b.int_col limit 500')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end
