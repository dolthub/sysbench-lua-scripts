require("oltp_common")

function prepare_statements()
	prepare_delete_inserts()
end

function event()
	execute_delete_inserts()
end

