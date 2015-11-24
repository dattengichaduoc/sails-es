require "colors"
elasticsearch = require "elasticsearch"
debug = require("debug")("WATERLINE-SAILS-ES")
debug.error = require("debug")("WATERLINE-SAILS-ES-ERROR")
async = require "async"
_ = require "lodash"
uuid = require "node-uuid"
module.exports = {
	find : (connections, conn, coll, options, cb)->
		debug coll, options
		client = connections[conn]
		query = new Object()
		query.index = client.options.index
		query.type = coll
		query.body = {}
		f_query = undefined

		if options.limit
			query.body.size = options.limit

		if options.skip
			query.body.from = options.skip


		if options.sort
			query.sort = []
			for k, v of options.sort
				try
					sortObject = JSON.parse(k)
					options.sort = sortObject
				catch e
					options.sort = options.sort
			for k, v of options.sort
				t = "asc"
				if parseInt(v) is -1
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
			# query.q = "_class:#{options.where._class}"
			
			# if options.where 

			#debug "TRACE".red, JSON.stringify options, null, 2

			match_query = []
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
				
			query.body.query = {}
			
			query.body.query.bool = {}
			
			if _must.must
				query.body.query.bool.must = _must.must
			if _must_not.must_not
				query.body.query.bool.must_not = _must_not.must_not
			if _should.should
				query.body.query.bool.should = _should.should
			if _filters.length > 0
				query.body.query.bool.must = _filters
				
			f_query = "search"
			
			### BUILD QUERY  ####
#
		 #debug "QUERY", f_query, query
		 #debug "MUST", _must
		 #debug "MUST NOT", _must_not
		 #debug "SHOULD", _should

		##### QUERY #####
		#debug JSON.stringify(f_query, null,2), JSON.stringify(query,null,2)
		# debug "FIND BUILD", query
		query.version = true
		client[f_query] query
		.then (results)->
			#debug JSON.stringify(f_query, null,2), JSON.stringify(query,null,2), JSON.stringify(results, null, 2)
			if results._source
				_data = results._source
				_data.counter = results._version
				return cb null, data
			
			else if results.docs
				docs = _.map results.docs, (d)->
					_data_do	= d._source
					_data_doc.counter = d._version
					_data_doc
				return cb null,	docs

			else if results.hits
				docs = _.map results.hits.hits, (d)->
					_data_doc	= d._source
					_data_doc.counter	= d._version
					_data_doc
				return cb null,	docs

		.catch (errors)->
			#console.log errors.toString()
			cb errors.toString()
}