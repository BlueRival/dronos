"use strict";

module.exports.init = function ( conf ) {

	var options = {
		prefix: conf.prefix || null,
		mongoose: conf.mongoose || null
	};

	if ( typeof options.prefix !== 'string' ) {
		options.prefix = "";
	}

	if ( !options.mongoose ) {
		return null;
	}

	var mongoose = options.mongoose;
	var Schema = mongoose.Schema;

	// actual schema
	var DronosSchema = new Schema(
		{
			account:           { type: String, required: true }, // the account that owns the chron entry, type is string to support some meta entries that are not exactly ObjectIds
			name:              { type: String, required: true }, // the name of this chron entry
			lastUpdate:        { type: Date, required: true }, // the next time the chron should run
			nextRun:           { type: Date, required: true }, // the next time the chron should run
			lastRun:           { type: Date, required: true }, // the last time the chron was run
			repeat:            { type: Boolean, default: true }, // should the chron item continue to run after it has run once
			recurrencePattern: { // a pattern to identify when this chron should run, matches crontab patterns of cron systems in Linux/Unix
				minute:     { type: String, required: true, default: '0' }, // 0 - 59
				hour:       { type: String, required: true, default: '*' }, // 0 -23
				dayOfMonth: { type: String, required: true, default: '*' }, // 1 - 31
				dayOfWeek:  { type: String, required: true, default: '*' }, // 0 - 6 is sunday - saturday
				month:      { type: String, required: true, default: '*' }, // 1 - 12 is Jan. - Dec.
				year:       { type: String, required: true, default: '*' }  // 2012 - ∞
			},
			lib:               { type: String, required: true }, // the module in the lib directory to use, must export the method specified in the method field
			method:            { type: String, required: true }, // the method to call in the lib, must have prototype of function(params, function(err) {}); where params is passed from the params field, and function(err) {} is the finished call back, where err should be null|undefined|false if execution completed successfully, otherwise it should be a string containing the error message
			params:            { type: Schema.Types.Mixed, default: {} }, // any value, it will be passed as the first argument to the method
			processingLimit:   { type: Number, required: true, default: 0 }, // max # parallel executions, 0 (zero) for infinite
			processingCount:   { type: Number } // current # of parallel executions
		},
		{
			strict: true
		}
	);

	// indexes
	DronosSchema.index( { account: 1, name: 1 }, { unique: true } );
	DronosSchema.index( { account: 1, name: 1, lastUpdate: -1 } );
	DronosSchema.index( { nextRun: 1, processingCount: 1, processingLimit: 1 } );

	// store schema and exchange schema for model
	return mongoose.model( options.prefix + 'Dronos', DronosSchema );

};


