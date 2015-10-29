require "colors"
elasticsearch = require "elasticsearch"
debug = require("debug")("WATERLINE-SAILS-ES")
debug.error = require("debug")("WATERLINE-SAILS-ES-ERROR")
async = require "async"
_ = require "lodash"
uuid = require "node-uuid"

module.exports = do ->

	connections = {}
	adapter = 
		syncable: false
		defaults: {
			host: "127.0.0.1"
			port: "9200"
		}

		##################################
		############ REGISTER ############
		##################################

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


		##################################
		############ TEAR DOWN ###########
		##################################
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

		##################################
		############ DESCRIBE ############
		##################################
		describe: (conn, coll, cb) ->
			debug "describe".warn.bold, conn, coll
			cb()

		##################################
		############ DEFINE   ############
		##################################
		define: (conn, coll, definition, cb) ->
			debug "define".warn.bold, conn, coll, definition
			cb()

		##################################
		############ DROP     ############
		##################################
		drop: (conn, coll, relations, cb) ->

			debug "drop".warn.bold, conn, coll, relations

			client = connections[conn]

			client.search
				index: client.options.index
				type: coll
				q: "*"
			.then (results)->
				async.eachLimit results.hits.hits, 10, (id, next)->
					client.delete
						index: client.options.index
						type: coll
						id: id._id
					.then (results)->
						next null
					.catch next
				, cb
			.catch cb

		##################################
		############ FIND     ############
		##################################
		find: (conn, coll, options, cb) ->

			debug "find".warn.bold, conn, coll, JSON.stringify(options, null, 2)

			client = connections[conn]
			query = new Object()
			query.index = client.options.index
			query.type = coll
			query.body = {}
			f_query = undefined

			if options.limit
				query.body = {}
				query.body.size = options.limit

			if options.skip
				query.body.from = options.skip

			if options.sort
				query.sort = []
				for k, v of options.sort
					t = "asc"
					if v is -1
						t = "desc"
					query.sort.push "#{k}:#{t}"

			if options.where and options.where.id
				if typeof options.where.id is "object"
					query.body.ids = options.where.id
					f_query = "mget"
				else
					query.id = options.where.id
					delete query.body
					f_query = "get"
			else
				unless options.limit
					query.body.size = 50
				unless options.skip
					query.body.from = 0
					
				f_query = "search"

			debug "QUERY", f_query, query

			##### QUERY #####
			client[f_query] query
			.then (results)->
				debug results
				if results._source
					return cb null, results

				if results.docs
					return cb null,	results.docs

				if results.hits
					return cb null, results.hits.hits
			.catch (errors)->
				cb errors.toString()


		##################################
		############ CREAT ###############
		##################################
		create: (conn, coll, values, cb) ->

			debug "create".warn.bold, conn, coll, "\r\n" ,values
			
			client = connections[conn]

			unless values.id
				values.id = uuid.v1()

			async.waterfall [
				(next)->
					client.create
						id: values.id
						index: client.options.index
						type: coll
						body: values
					.then (results)->
						next null, results
					.catch (errors)->
						next errors
			],(errors, results)->
				if errors
					cb errors
				else
					debug JSON.stringify results, null, 2
					cb null

		##################################
		############ UPDATE ##############
		##################################
		update: (conn, coll, options, values, cb) ->
			debug "update".warn.bold, conn, coll, options, values
			cb()

		##################################
		############ DESTROY #############
		##################################
		destroy: (conn, coll, options, cb) ->
			debug "destroy".warn.bold, conn, coll, options
			cb()

	adapter