require "colors"
elasticsearch = require "elasticsearch"
debug = require("debug")("WATERLINE-SAILS-ES")
debug.error = require("debug")("WATERLINE-SAILS-ES-ERROR")
async = require "async"
_ = require "lodash"
uuid = require "node-uuid"
module.exports = {
	find : (connections, conn, coll, options, cb)->
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

			for key, value of options.where
				debug key,value
				switch key
					when "or"
						console.log "NOT SUPPORT `or` NOW"
					when "like"
						console.log "NOT SUPPORT `like` NOW"
					else
						match_query.push { "#{key}" : value }
						unless bool_check.must
							query.body.query = {}
							query.body.query.bool = {}
							query.body.query.bool["must"] = []
							query.body.query.bool["must"].push match_query
						else
							query.body.query.bool["must"].push match_query
						debug JSON.stringify(query, null, 2)
			f_query = "search"


			### BUILD QUERY  ####

		 #debug "QUERY", f_query, query

		##### QUERY #####
		#debug JSON.stringify(f_query, null,2), JSON.stringify(query,null,2)
		client[f_query] query
		.then (results)->
			#debug JSON.stringify(f_query, null,2), JSON.stringify(query,null,2), JSON.stringify(results, null, 2)
			if results._source
				return cb null, [results._source]

			if results.docs
				docs = _.map results.docs, (d)->
					d._source
				return cb null,	docs

			if results.hits
				docs = _.map results.hits.hits, (d)->
					d._source
				return cb null, docs

		.catch (errors)->
			#console.log errors.toString()
			cb errors.toString()
}