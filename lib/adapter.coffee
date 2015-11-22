require "colors"
elasticsearch = require "elasticsearch"
debug = require("debug")("WATERLINE-SAILS-ES")
debug.error = require("debug")("WATERLINE-SAILS-ES-ERROR")
async = require "async"
_ = require "lodash"
uuid = require "node-uuid"
database = require "./database.coffee"

module.exports = do ->

	connections = {}
	adapter = 
		identity : "waterline-sails-es"
		syncable: false
		pkFormat: "string"
		defaults: {
			host: "127.0.0.1"
			port: "9200"
		}

		##################################
		############ REGISTER ############
		##################################

		registerConnection: (connection, colls, cb) ->
			#debug "registerConnection".warn.bold
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

							for k, v of type.map
								if v.collection or v.model
									delete type.map[k]
							option = 
								index: connection.index
								type: type.name
								body: 
									properties: type.map
							# debug JSON.stringify option, null, 2
							client.indices.putMapping option
							.then (results)->
								debug "mapping successfuly #{type.name.green.bold}", JSON.stringify results
								next null
							.catch (error)->
								debug.error JSON.stringify(type.map, null,2) , JSON.stringify error, null, 2
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
						#(next)->
							#fn_PutMapping next
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
			#debug "teardown".warn.bold, conn, "DISCONNECTED ELASTICSEARCH".cyan.bold
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

		count: (conn, coll, options, cb)->
			debug "count", conn, coll, options

			#define connection
			client = connections[conn]
			option = {}
			option.index = client.options.index
			option.type = coll
			option.body ={}
			option.body.query = {}
			option.body.query.bool = {}
			match_query = {}
			type_query = "must"
			bool_check = {
				must: false
				must_not: false
				should: false
			}
			
			_must = {}
			_must_not = {}
			_should = {}
			
			fn_query_refilter = (key,value)->
				data = []
				switch key
					when "match"
						for k,v of value
							data.push {
								match: 
									"#{k}": v
							}
					when "range"
						for k,v of value
							data.push {
								range: 
									"#{k}": v
							}
					when "query_string"
						for k,v of value
							data.push {
								query_string: 
									"default_field": k
									"query": v
							}
					when "temp"
						for k,v of value
							data.push {
								temp: 
									"#{k}": v
							}
					when "prefix"
						for k,v of value
							data.push {
								prefix: 
									"#{k}": v
							}
					when "wildcard"
						for k,v of value
							data.push {
								wildcard: 
									"#{k}": v
							}
					when "must"
						values = []
						for k, v of value
							values = values.concat fn_query_refilter(k, v)
						data = {
							must: values
						}
					when "must_not"
						values = []
						for k, v of value
							values = values.concat fn_query_refilter(k, v)
						data = {
							must_not: values
						}
					when "should"
						values = []
						for k, v of value
							values = values.concat fn_query_refilter(k, v)
						data = {
							should: values
						}
					else 
						return {}
				return data
			
			_filters = []
			
			filterGroups = _.forEach options.where, (value, key)->
				try
					value = JSON.parse value
				catch e
					value = value
				
				switch key
					when "must"
						parses = {}
						unless Array.isArray(value)
							value = [value]
						for p in value
							try
								p = JSON.parse p
								parses[_.keys(p)[0]] = p[_.keys(p)[0]]
								
							catch
								parses[_.keys(p)[0]] = p[_.keys(p)[0]]
						value = parses
						_must = fn_query_refilter(key,value)
						
					when "must_not"
						parses = {}
						unless Array.isArray(value)
							value = [value]	
						for p in value
							try
								p = JSON.parse p
								parses[_.keys(p)[0]] = p[_.keys(p)[0]]
								
							catch
								parses[_.keys(p)[0]] = p[_.keys(p)[0]]
						value = parses
						_must_not = fn_query_refilter(key,value)
					when "should"
						parses = {}
						unless Array.isArray(value)
							value = [value]	
						for p in value
							try
								p = JSON.parse p
								parses[_.keys(p)[0]] = p[_.keys(p)[0]]
								
							catch
								parses[_.keys(p)[0]] = p[_.keys(p)[0]]
						value = parses
						_should = fn_query_refilter(key,value)
					else
						_filters = _filters.concat(fn_query_refilter(key,value))
				return
				
			option.body.query = {}
			
			option.body.query.bool = {}
			
			if _must.must
				option.body.query.bool.must = _must.must
			if _must_not.must_not
				option.body.query.bool.must_not = _must_not.must_not
			if _should.should
				option.body.query.bool.should = _should.should
			if _filters.length > 0
				option.body.query.bool.must = _filters
				
			f_query = "count"
			debug "COUNT BUILD", option
			client[f_query] option
			.then (results)->
				debug "COUNT", results
				cb null, results.count
			.catch (e)->
				debug "COUNT", results
				
				
		##################################
		############ DESCRIBE ############
		##################################
		describe: (conn, coll, cb) ->
			#debug "describe".warn.bold, conn, coll
			cb()

		##################################
		############ DEFINE   ############
		##################################
		define: (conn, coll, definition, cb) ->
			#debug "define".warn.bold, conn, coll, definition
			cb()

		##################################
		############ DROP     ############
		##################################
		drop: (conn, coll, relations, cb) ->

			#debug "drop".warn.bold, conn, coll, relations

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

			#debug "find".warn.bold, conn, coll, JSON.stringify(options, null, 2)

			database.find connections, conn, coll, options, cb
		
		##################################
		############ DESTROY #############
		##################################
		join: (conn, coll, options, cb)->

			#debug "join".warn.bold, conn, coll, JSON.stringify(options, null, 2)

			database.find connections, conn, coll, options, (errors, results)->
				if errors
					cb errors
				else
					async.eachLimit results, 1,(data, done)->
						async.each options.joins, (relation, next)->
							if relation.childKey is "id" or relation.parentKey is "id"
								if relation.collection
									sails.models[relation.child].find().where
										match:
											"#{relation.childKey}": data.id
									.then (list)->
										data["#{relation.alias}"] = list
										next null
								else
									if data["#{relation.alias}"]
										sails.models[relation.child].find
											id: data["#{relation.alias}"]
										.then (node)->
											data["#{relation.alias}"] = node
											next null
									else
										next null
							else
								next null
						,(errors)->
							# debug.error JSON.stringify(results, null, 2)
							done null
					,(errors)->
						cb null, results
			
		##################################
		############ CREAT ###############      
		##################################
		create: (conn, coll, values, cb) ->

			#debug "create".warn.bold, conn, coll, values
			
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
				sails.models[coll].findOne results._id
				.then (_results)->
					async.eachLimit sails.models[coll].associations, 1, (ass, done)->
						_results[ass.alias] = new Array()
						if ass.type is "collection"
							_results[ass.alias].add = (new_children)->
								if typeof new_children is "object"
									sails.models[ass.collection].create new_children
									.then (___result)->
										sails.models[ass.collection].findOne ___result.id
									.then (____result)->
										_results[ass.alias].push ____result
								else
									sails.models[ass.collection].findOne new_children
									.then (___result)->
										_results[ass.alias].push ___result
							done null
						else 
							done null
					,(__errors)->
						cb null, _results
				.catch (_errors)->
					# debug.error "ERROR", "create::async.waterfall::.findOne", errors
					cb _errors


		##################################
		############ UPDATE ##############
		##################################
		update: (conn, coll, options, values, cb) ->
			
			unless options.where.id
				return cb null
				
			debug "update".warn.bold, conn, coll, options, values
			
			client = connections[conn]
			values.updatedAt = new Date()
			__options = 
				index: client.options.index
				type: coll
				id: values.id
				body:
					doc: values
			debug "UPDATE NOW", __options
			client.update __options
			.then((results)->
				debug "UPDATE", results
				cb null
			).catch((errors)->
				debug "UPDATE ERRORS", errors
				cb errors
			)

		##################################
		############ DESTROY #############
		##################################
		##################################
		destroy: (conn, coll, options, cb) ->
			#debug "destroy".warn.bold, conn, coll, options
			cb()
	adapter

