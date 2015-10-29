require "colors"
elasticsearch = require "elasticsearch"
debug = require("debug")("WATERLINE-SAILS-ES")
debug.error = require("debug")("WATERLINE-SAILS-ES-ERROR")
async = require "async"
_ = require "lodash"

module.exports = do ->

	connections = {}
	adapter = 
		syncable: false
		defaults: {
			host: "127.0.0.1"
			port: "9200"
		}

		registerConnection: (connection, colls, cb) ->
			debug "registerConnection".warn.bold
			typies = []
			for k, v of colls
				typies.push 
					name : k
					map: v._attributes
					migrate: v.migrate

			if !connection.identity
				return cb(new Error("Connection is missing an identity."))
			if connections[connection.identity]
				return cb(new Error("Connection is already registered."))

			connections[connection.identity] = new elasticsearch.Client connection
			connections[connection.identity].ping {}, (e,results)->
				connections[connection.identity].options = connection

				if e
					debug "-".red.bold, JSON.stringify(results, null, 2)
				else
					debug "OK".green.bold, "connected elasticsearch #{connection.host or connection.hosts[0]} !!!".green.bold
					
					client = connections[connection.identity]

					debug "create index #{connection.index}"

					fn_CreateIndex = (done)->
						client.indices.create 
							index: connection.index
						.then (results)->
							debug "created #{connection.index} ...", results
							done null
						.catch (error)->
							debug "#{connection.index} already exists ..."
							done null
						
					fn_DropCollection = (done)->
						async.each typies, (type, next)->
							if type.migrate is "drop"
								colls[type.name].drop((error)->
									next null
								)
							else
								next null
						,(errors)->
							done errors

					fn_PutMapping = (done)->
						async.each typies, (type, next)->
							option = 
								index: connection.index
								type: type.name
								body: 
									properties: type.map
							client.indices.putMapping option
							.then (results)->
								debug "mapping successfuly #{type.name.green.bold}", JSON.stringify results
								next null
							.catch (error)->
								debug.error JSON.stringify error, null, 2
								next null
						,(errors)->
							if errors
								debug.error JSON.stringify errors, null, 2
								done errors
							else 
								debug "mapping all typies successfuly !"
								done null

					async.waterfall [
						(next)->
							fn_CreateIndex next
						(next)->
							fn_DropCollection next
						(next)->
							fn_PutMapping next							
					], (errors)->
						if errors
							debug.error "STARTING ERROR".red.bold , JSON.stringify(errors, null, 2)
							cb errors
						else
							debug "Initialized adapter #{connection.adapter.green.bold}"
							cb()
			return

		teardown: (conn, cb) ->
			debug "teardown".warn.bold, conn, "DISCONNECTED ELASTICSEARCH".cyan.bold
			if typeof conn is "function"
				cb = conn
				conn = null
			if !conn
				connections = {}
				return cb()
			if !connections[conn]
				return cb()
			delete connections[conn]
			cb()
			return

		describe: (conn, coll, cb) ->
			debug "describe".warn.bold, conn, coll
			cb()

		define: (conn, coll, definition, cb) ->
			debug "define".warn.bold, conn, coll, definition
			cb()

		drop: (conn, coll, relations, cb) ->
			debug "drop".warn.bold, conn, coll, relations
			connections[conn].indices.delete
				index: "#{connections[conn].options.index}/#{coll}"
			.then (results)->
				cb null
			.catch cb

		find: (conn, coll, options, cb) ->
			debug "find".warn.bold, conn, coll, options
			cb()

		create: (conn, coll, values, cb) ->
			debug "create".warn.bold, conn, coll, values
			cb()

		update: (conn, coll, options, values, cb) ->
			debug "update".warn.bold, conn, coll, options, values
			cb()

		destroy: (conn, coll, options, cb) ->
			debug "destroy".warn.bold, conn, coll, options
			cb()

	adapter