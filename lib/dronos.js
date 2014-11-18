"use strict";

var __ = require( 'doublescore' );
var async = require( 'async' );
var later = require( 'later' );
var moment = require( 'moment' );
var mongoose = require( 'mongoose' );

/**
 *
 * @param params
 * @constructor
 */
var Dronos = function( params ) {

	this._params = params = {
		mongodb:   params.mongodb || null,
		prefix:    params.prefix || '',
		batchSize: params.batchSize || 5
	};

	if ( typeof params.mongodb !== 'string' || params.mongodb.length < 1 ) {
		throw new Error( 'mongodb must be a non-empty string' );
	}

	this._init();

};

Dronos.prototype._init = function() {

	var self = this;

	self._running = false;
	self._runnerTimeout = null;

	self._handlers = {};

	var mongooseConfig = {
		prefix:   self._params.prefix,
		db:       mongoose.createConnection( self._params.mongodb ),
		mongoose: mongoose
	};

	self._models = {};

	[ 'dronos' ].forEach( function( modelName ) {
		var className = modelName.charAt( 0 ).toUpperCase() + modelName.slice( 1 );
		self._models[ className ] = require( './models/' + modelName ).init( mongooseConfig );

		if ( self._models[ className ] === null ) {
			throw new Error( 'Could not load model ' + modelName );
		}
	} );

};

/**
 * @callback Dronos~var var done
 * @param {?Error} err - If an error occurred, this is the Error object.
 * @param {?*} data - If no error occurred this is optionally data, or null if no data expected.
 */

/**
 * @callback Dronos~handler
 * @param {?object.<string,*>} params - Arbitrary parameters that were set when with the schedule.
 * @param {done} done - A function to call when the handler is done processing the event.
 */

/**
 * Set Schedule
 *
 * Upserts a schedule.
 *
 * @param {object} schedule
 *        {string} schedule.owner - The id of the owner of the schedule. The combination of owner and name must be unique.
 *        {string} schedule.name - The name of the schedule. The combination of owner and name must be unique.
 *        {string} schedule.recurrence - A cron compatible specification for the recurrence pattern http://en.wikipedia.org/wiki/Cron#Examples
 *        {Date} schedule.start - Run this schedule only on dates after the this one.
 *        {Date} schedule.end - Run this schedule only on dates before the this one.
 *        {?object.<string,*>} schedule.params - Arbitrary parameters to pass to the handler function when running instances.
 * @param {done} done - Called after the schedule entry has been upserted or denied.
 *
 */
Dronos.prototype.set = function( schedule, done ) {

	if ( typeof done !== 'function' ) {
		throw new Error( 'done should be a function' );
	}

	var _done = function( err, schedule ) {
		if ( err ) {
			done( new Error( err ) );
		} else {
			done( null, schedule || null );
		}
	};

	if ( !_validateSchedule( schedule, _done ) ) {
		return;
	}

	var nextRun = getNextRun( schedule );

	if ( nextRun === null ) {
		_done( 'no next run time' );
		return;
	}

	schedule._nextRun = nextRun;
	schedule._lastUpdate = new Date();

	this._models.Dronos.findOneAndUpdate(
		{
			owner: schedule.owner,
			name:  schedule.name
		},
		{
			$set:         schedule,
			$inc:         {
				_version: 1
			},
			$setOnInsert: {
				_lastRun: new Date( '2010-10-01T00:00:00Z' )
			}
		},
		{
			upsert: true,
			new:    true
		},
		function( err, schedule ) {
			if ( !err && !schedule ) {
				err = new Error( 'failed to store schedule' );
			}
			if ( err ) {
				_done( err );
			} else {
				_done( null, schedule.toObject() );
			}
		}
	);

};

/**
 * Get Schedule
 *
 * Gets a schedule, with all meta data field prefixed with _.
 *
 * @param {object} schedule
 *        {string} schedule.owner - The id of the owner of the schedule. The combination of owner and name must be unique.
 *        {string} schedule.name - The name of the schedule. The combination of owner and name must be unique.
 * @param {done} done - Called after the schedule entry has been removed. If removal command was a success, the data field will be true if an existing entry was deleted, or false if no entry matched the owner/name combination.
 *
 */
Dronos.prototype.get = function( schedule, done ) {

	if ( !validateScheduleBasicFields( schedule, done ) ) {
		return;
	}

	this._models.Dronos.findOne( {
									 owner: schedule.owner || '',
									 name:  schedule.name || ''
								 }, function( err, schedule ) {
		if ( err ) {
			done( err );
		} else {
			done( null, schedule ? schedule.toObject() : null );
		}
	} );

};

/**
 * Remove Schedule
 *
 * Removes a schedule immediately, even if there are handlers currently running.
 *
 * @param {object} schedule
 *        {string} schedule.owner - The id of the owner of the schedule. The combination of owner and name must be unique.
 *        {string} schedule.name - The name of the schedule. The combination of owner and name must be unique.
 * @param {done} done - Called after the schedule entry has been removed. If removal command was a success, the data field will be true if an existing entry was deleted, or false if no entry matched the owner/name combination.
 *
 */
Dronos.prototype.remove = function( schedule, done ) {

	if ( !validateScheduleBasicFields( schedule, done ) ) {
		return;
	}

	this._models.Dronos.remove( {
									owner: schedule.owner || '',
									name:  schedule.name || ''
								}, function( err, count ) {
		if ( err ) {
			done( err );
		} else {
			done( null, count > 0 );
		}
	} );

};

/**
 * Listen to all Schedules
 *
 * Listens to all schedules regardless of owner and name. This is useful for
 * supplying your own routing logic, or for ensuring all nodes run all
 *
 * @param {handler} run - The handler to run when the schedule runs an instance.
 * @return {boolean} TRUE for success, FALSE for failure
 */
Dronos.prototype.listenAll = function( run ) {

	if ( typeof run !== 'function' ) {
		return false;
	}

	this._handlers = run;
	return true;

};

/**
 * Listen to a Schedule
 *
 * @param {object} schedule
 *        {string} schedule.owner - The id of the owner of the schedule. The combination of owner and name must be unique.
 *        {string} schedule.name - The name of the schedule. The combination of owner and name must be unique.
 * @param {handler} run - The handler to run when the schedule runs an instance.
 * @param {done} done
 */
Dronos.prototype.listen = function( schedule, run, done ) {

	if ( typeof done !== 'function' ) {
		throw new Error( 'run field must be a function' );
	}

	if ( !validateScheduleBasicFields( schedule, done ) ) {
		return;
	}

	if ( typeof run !== 'function' ) {
		done( new Error( 'run field must be a function' ) );
		return;
	}

	var self = this;

	self.get( schedule, function( err, schedule ) {

		if ( err || schedule === null ) {
			done( new Error( 'schedule doesn\'t exist' ) );
			return;
		}

		var key = handlerKey( schedule );
		var type = typeof self._handlers;

		if ( type !== 'object' || type === 'function' ) {
			self._handlers = {};
		}

		self._handlers[ key ] = run;

		done();

	} );

};

Dronos.prototype.start = function() {

	var self = this;

	if ( self._running ) {
		return;
	}
	self._running = true;

	var go = function() {
		// if no longer running, nothing to do
		if ( self._running === false ) {
			return;
		}

		self._runReadySchedules( function() {

			// if no longer running, nothing to do
			if ( self._running === false ) {
				return;
			}

			// calculate offset to wait until top of next minute
			var nextRun = getNextRun( { pattern: '* * * * * * ' } );
			var offset = nextRun.valueOf() - moment().valueOf();
			if ( offset < 0 ) {
				offset = 1;
			}

			// set a timeout to wait until top of next minute to look for
			// more searches
			self._runnerTimeout = setTimeout( function() {
				self._runnerTimeout = null;
				go();
			}, offset );

		} );
	};

	go();

};

Dronos.prototype.stop = function() {

	var self = this;

	self._running = false;

	if ( self._runnerTimeout ) {
		clearTimeout( self._runnerTimeout );
	}
	self._runnerTimeout = null;

};

Dronos.prototype._runReadySchedules = function( done ) {

	var self = this;

	// TODO convert this nested callback nightmare to async.series calls

	// get all possible schedules ready to run
	self._models.Dronos.find( {
								  _nextRun: { $lt: moment().toISOString() }
							  },
							  function( err, schedules ) {

								  if ( self._running === false ) {
									  done();
									  return;
								  }

								  if ( !Array.isArray( schedules ) || schedules.length < 1 ) {
									  done();
									  return;
								  }

								  // if we got some schedules to run, and we are not shutting down,
								  // run each in parallel according to batch size.
								  async.eachLimit( schedules, self._params.batchSize, function( schedule, done ) {

									  // before attempting to run each schedule, see if we are shutting down
									  if ( self._running === false ) {
										  done( true );
										  return;
									  }

									  // mark the schedule as running if it is still due
									  self._models
										  .Dronos
										  .findOneAndUpdate( {
																 _id:      schedule._id,
																 _nextRun: { $lt: moment().toISOString() }
															 },
															 {
																 $set: {
																	 _lastRun: moment().toISOString(),
																	 _nextRun: getNextRun( schedule )
																 }
															 },
															 { upsert: false },
															 function( err, schedule ) {

																 // another instance may have reached this schedule first
																 if ( schedule ) {

																	 self._runSchedule( schedule, function( foundHandler ) {

																		 // the handler may have been removed since last run,
																		 if ( foundHandler ) {
																			 done();
																		 } else {

																			 // handler was removed, reset the entry
																			 self._models
																				 .Dronos
																				 .findOneAndUpdate( {
																										_id: schedule._id
																									},
																									{
																										$set: {
																											_lastRun: schedule._lastRun,
																											_nextRun: schedule._nextRun
																										}
																									},
																									{ upsert: false },
																									function() {
																										done();
																									} );

																		 }

																	 } );
																 } else {
																	 done();
																 }

															 } );

								  }, function() {
									  done();
								  } );

							  } );

};

Dronos.prototype._runSchedule = function( schedule, done ) {

	var self = this;
	var runner = null;
	var key = handlerKey( schedule );

	if ( typeof self._handlers === 'function' ) {
		runner = self._handlers;
	} else if ( self._handlers && typeof self._handlers === 'object' && typeof self._handlers[ key ] === 'function' ) {
		runner = self._handlers[ key ];
	}

	if ( typeof runner !== 'function' ) {
		done( false );
		return;
	}

	// run the handler in the context of the schedule object
	runner.call( schedule, schedule, function() {
		done( true );
	} );

};

module.exports = Dronos;

function handlerKey( schedule ) {
	return JSON.stringify( {
							   owner: schedule.owner || null,
							   name:  schedule.name || null
						   } );
}

function _validateScheduleBasicFields( schedule, done ) {

	if ( !schedule || typeof schedule !== 'object' || Array.isArray( schedule ) ) {
		done( 'schedule is a required object parameter' );
		return false;
	}

	if ( ![ 'owner', 'name' ].every( function( field ) {
			var result = schedule.hasOwnProperty( field ) &&
						 typeof schedule[ field ] === 'string' &&
						 schedule[ field ].length > 0;
			if ( !result ) {
				done( field + ' is a required string parameter' );
			}
			return result;
		} ) ) {
		return false;
	}

	return true;
}

function validateScheduleBasicFields( schedule, done ) {
	return _validateScheduleBasicFields( schedule, function( err ) {
		if ( err ) {
			done( new Error( err ) );
		} else {
			done( null );
		}
	} );
}

function _validateSchedule( schedule, done ) {

	if ( !_validateScheduleBasicFields( schedule, done ) ) {
		return false;
	}

	if ( ![ 'recurrence' ].every( function( field ) {
			var result = schedule.hasOwnProperty( field ) &&
						 typeof schedule[ field ] === 'string' &&
						 schedule[ field ].length > 0;
			if ( !result ) {
				done( field + ' is a required string parameter' );
			}
			return result;
		} ) ) {
		return false;
	}

	if ( ![ 'start', 'end' ].every( function( field ) {

			if ( !schedule.hasOwnProperty( field ) ) {
				return true;
			}

			if ( schedule[ field ] instanceof moment ) {
				return true;
			}

			if ( typeof schedule[ field ] === 'string' && schedule[ field ].match( /^[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}:[0-9]{2}\.?[0-9]{0,3}Z/ ) ) {
				schedule[ field ] = moment( schedule[ field ] );
				return true;
			}

			if ( typeof schedule[ field ] === 'object' && typeof schedule[ field ].toISOString === 'function' ) {
				schedule[ field ] = moment( schedule[ field ].toISOString() );
				return true;
			}

			done( field + ' is not a valid time string, Date instance or moment instance' );
			return false;

		} ) ) {
		return false;
	}

	return true;
}

function getNextRun( schedule ) {
	var s = later.parse.cron( schedule.recurrence );

	var args = [ 1 ];
	if ( schedule.hasOwnProperty( 'start' ) ) {
		args.push( schedule.start );
	}

	s = later.schedule( s );
	var nextRun = moment( s.next.apply( s, args ).toISOString() );

	if ( schedule.hasOwnProperty( 'end' ) && moment( schedule.end ).isBefore( nextRun ) ) {
		return null;
	}

	return nextRun.millisecond( 0 ).seconds( 0 );
}
