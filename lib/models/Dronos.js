"use strict";

module.exports.init = function( params ) {

	params = {
		prefix:   params.prefix || null,
		db:       params.db || null,
		mongoose: params.mongoose || null
	};

	if ( typeof params.prefix !== 'string' ) {
		params.prefix = '';
	}
	params.prefix = params.prefix.trim();

	if ( !params.db ) {
		throw new Error( 'db is a required model field' );
	}

	if ( !params.mongoose ) {
		throw new Error( 'mongoose is a required model field' );
	}

	var mongoose = params.mongoose;
	var Schema = mongoose.Schema;

	// actual schema
	var DronosSchema = new Schema(
		{
			owner:       { type: String, required: true }, // the id of the owner of the schedule
			name:        { type: String, required: true }, // the name of this schedule,
			recurrence:  { type: String, required: true }, // the cron compatible specification for the recurrence pattern http://en.wikipedia.org/wiki/Cron#Examples
			start:       { type: Date, default: null }, // repeat the schedule after the specified time
			end:         { type: Date, default: null }, // repeat the schedule until the specified time
			params:      { type: {}, default: {} }, // meta information to associate with the task. This is passed to the execution instance
			_version:    { type: Number, required: true, default: 0 }, // tracks the version of the scheduled item
			_lastUpdate: { type: Date, required: true }, // the last time the schedule was changed
			_nextRun:    { type: Date, required: true }, // the next time the schedule should run
			_lastRun:    { type: Date, required: true } // the last time the schedule was run
		},
		{
			id:         false,
			strict:     true,
			versionKey: false
		}
	);

	// indexes
	DronosSchema.index( { owner: 1, name: 1 }, { unique: true } );
	DronosSchema.index( { owner: 1, name: 1, _lastUpdate: -1 } );
	DronosSchema.index( { _nextRun: 1, _lastRun: 1 } );

	var modelName = 'dronos';

	if ( params.prefix.length > 0 ) {
		modelName = params.prefix + 'Dronos';
	}

	// set the schema
	return params.db.model( modelName, DronosSchema, modelName );

};


