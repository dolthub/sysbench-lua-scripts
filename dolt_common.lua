
-- -----------------------------------------------------------------------------
-- Common code for dolt benchmarks.
-- -----------------------------------------------------------------------------

function init()
    assert(event ~= nil, "this script is meant to be included by other OLTP scripts and should not be called directly.")
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepare, warmup, run, cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
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
	id INT NOT NULL,
	tiny_int_col TINYINT NOT NULL,
	unsigned_tiny_int_col TINYINT UNSIGNED NOT NULL,
	small_int_col SMALLINT NOT NULL,
	unsigned_small_int_col SMALLINT UNSIGNED NOT NULL,
	medium_int_col INT NOT NULL,
	unsigned_medium_int_col INT UNSIGNED NOT NULL,
	int_col INT NOT NULL,
	unsigned_int_col INT UNSIGNED NOT NULL,
	big_int_col BIGINT NOT NULL,
	unsigned_big_int_col BIGINT UNSIGNED NOT NULL,

	float_col FLOAT NOT NULL,
	double_col DOUBLE NOT NULL,

	PRIMARY KEY(id),
	INDEX (big_int_col)
); ]] .. sysbench.opt.create_table_options)

    con:query(create_query)

    if (sysbench.opt.table_size > 0) then
        print(string.format("Inserting %d records into 'sbtest1'", sysbench.opt.table_size))
    end

    local query = [[INSERT INTO sbtest1
(id, tiny_int_col, unsigned_tiny_int_col, small_int_col, unsigned_small_int_col, medium_int_col, unsigned_medium_int_col, int_col, unsigned_int_col, big_int_col, unsigned_big_int_col, float_col, double_col)
VALUES
]]

    local str_vals = {"val0", "val1", "val2"}
    math.randomseed(0)

    con:bulk_insert_init(query)
    for i = 1, sysbench.opt.table_size do
        local row_values = "(" .. i .. "," ..                                  -- id
            math.random(-128, 127) .. "," ..                                   -- tiny_int_col
            math.random(0, 255) .. "," ..                                      -- unsigned_tiny_int_col
            math.random(-32768,  32767) .. "," ..                              -- small_int_col
            math.random(0, 65535) .. "," ..                                    -- unsigned_small_int_col
            math.random(-8388608, 8388607) .. "," ..                           -- medium_int_col
            math.random(0, 16777215) .. "," ..                                 -- unsigned_medium_int_col
            math.random(-2147483648, 2147483647) .. "," ..                     -- int_col
            math.random(0, 4294967295) .. "," ..                               -- unsigned_int_col
            math.random(-4611686018427387904, 4611686018427387903) .. "," ..   -- big_int_col
            math.random(0, 9223372036854775807) .. "," ..                      -- unsigned_big_int_col
            math.random() .. "," ..                                            -- float_col
            math.random() .. ")"                                               -- double_col

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



