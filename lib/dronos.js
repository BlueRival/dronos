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
 * @typedef {object} Dronos~scheduleParams
 * @property {string|number|null|undefined|array|Dronos~scheduleParams} * - Any arbitrary set of key/value properties which must only contain serializable data types
 */

/**
 * @typedef {object} Dronos~schedule
 * @param {object} schedule
 * @param {boolean} [schedule.enabled=TRUE] - Whether or not the schedule is active (will actually fire and iterate a recurrence).
 * @param {string} schedule.owner - The id of the owner of the schedule. The combination of owner and name must be unique.
 * @param{string} schedule.name - The name of the schedule. The combination of owner and name must be unique.
 * @param {string} schedule.recurrence - A cron compatible specification for the recurrence pattern http://en.wikipedia.org/wiki/Cron#Examples
 * @param {Date} [schedule.start=1969-12-31 23:59:59Z] - Run this schedule only on dates after the this one.
 * @param {Date} [schedule.end=] - Run this schedule only on dates before the this one.
 * @param {Dronos~scheduleParams} [schedule.params={}] - Arbitrary parameters to pass to the handler function when running instances.
 */

/**
 * @callback Dronos~handler
 * @param {Dronos~scheduleParams} params - Arbitrary parameters that were set with the schedule.
 * @param {done} done - A function to call when the handler is done processing the event.
 */

/**
 * @callback Dronos~basic
 * @param {Error|null|undefined} [err] - If there was an error, this will contain an error object, any other value indicates no error
 */

/**
 * @callback Dronos~scheduleEcho
 * @param {Error|null|undefined} [err] - If there was an error, this will contain an error object, any other value indicates no error
 * @param {Dronos~schedule} [schedule] - If there was no error, this will contain a valid schedule definition object.
 */

/**
 * @callback Dronos~ack
 * @param {Error|null|undefined} [err] - If there was an error, this will contain an error object, any other value indicates no error
 * @param {boolean} [ack] - If the command was possible and completed, TRUE, otherwise FALSE
 */

/**
 * Set Schedule
 *
 * Upserts a schedule.
 *
 * @param {schedule} schedule - The schedule pattern to set.
 * @param {Dronos~scheduleEcho} done - Called after the schedule entry has been upserted or denied.
 *
 */
Dronos.prototype.set = function( schedule, done ) {

	done = nonoop( done );

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

	/**
	 * @type {object}
	 * @augments Dronos~schedule
	 * @param {date} _nextRun - The next time the schedule should event
	 * @param {date} _lastUpdate - The last time the schedule record was modified
	 */
	var scheduleEntry = schedule;

	scheduleEntry._nextRun = nextRun.toDate();
	scheduleEntry._lastUpdate = new Date();

	// default this value
	if ( !scheduleEntry.hasOwnProperty( 'enabled' ) ) {
		scheduleEntry.enabled = true;
	}

	this._models.Dronos.findOneAndUpdate(
		{
			owner: scheduleEntry.owner,
			name:  scheduleEntry.name
		},
		{
			$set:         scheduleEntry,
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
 * @param {Dronos~scheduleEcho} done - Callback to receive error or the schedule definition.
 *
 */
Dronos.prototype.get = function( schedule, done ) {

	done = nonoop( done );

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
 * @param {Dronos~ack} done - Called after the schedule entry has been removed. If there was a matching schedule to delete, and no error occurred, TRUE, otherwise FALSE.
 *
 */
Dronos.prototype.remove = function( schedule, done ) {

	done = noop( done );

	if ( !validateScheduleBasicFields( schedule, done ) ) {
		return;
	}

	this._models.Dronos.remove( {
		owner: schedule.owner || '',
		name:  schedule.name || ''
	}, function( err, lastError ) {
		if ( err ) {
			done( err );
		} else {
			done( null, lastError && lastError.result && lastError.result.hasOwnProperty( 'n' ) && lastError.result.n > 0 );
		}
	} );

};

/**
 * Enable Schedule
 *
 * Enables a schedule to be evented.
 *
 * @param {object} schedule
 *        {string} schedule.owner - The id of the owner of the schedule.
 *        {string} schedule.name - The name of the schedule.
 * @param {Dronos~ack} done - Called after the schedule entry has been enabled. Will have data TRUE if a schedule was found and enabled, FALSE otherwise
 *
 */
Dronos.prototype.enable = function( schedule, done ) {

	done = noop( done );

	if ( !validateScheduleBasicFields( schedule, done ) ) {
		return;
	}

	this._models.Dronos.findOneAndUpdate(
		{
			owner: schedule.owner,
			name:  schedule.name
		},
		{
			$set: {
				enabled:     true,
				_lastUpdate: new Date()
			},
			$inc: {
				_version: 1
			}
		},
		{
			upsert: false,
			new:    true
		},
		function( err, schedule ) {
			if ( err ) {
				done( err );
				return;
			}

			done( null, !!(schedule && schedule.enabled) );
		}
	);
};

/**
 * Enable Schedule
 *
 * Enables a schedule to be evented.
 *
 * @param {object} schedule
 *        {string} schedule.owner - The id of the owner of the schedule.
 *        {string} schedule.name - The name of the schedule.
 * @param {Dronos~ack} done - Called after the schedule entry has been disabled. Will have data TRUE if a schedule was found and disabled, FALSE otherwise
 *
 */
Dronos.prototype.disable = function( schedule, done ) {

	done = noop( done );

	if ( !validateScheduleBasicFields( schedule, done ) ) {
		return;
	}

	this._models.Dronos.findOneAndUpdate(
		{
			owner: schedule.owner,
			name:  schedule.name
		},
		{
			$set: {
				enabled:     false,
				_lastUpdate: new Date()
			},
			$inc: {
				_version: 1
			}
		},
		{
			upsert: false,
			new:    true
		},
		function( err, schedule ) {
			if ( err ) {
				done( err );
				return;
			}

			done( null, !!(schedule && !schedule.enabled) );
		}
	);
};

/**
 * Listen to all Schedules
 *
 * Listens to all schedules regardless of owner and name. This is useful for
 * supplying your own routing logic, or for ensuring all nodes run all
 *
 * @param {Dronos~handler} run - The handler to run when the schedule runs an instance.
 * @return {boolean} TRUE for success registration, FALSE for failure.
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
 * @param {Dronos~handler} run - The handler to run when the schedule runs an instance.
 * @param {Dronos~basic} done - The call back fired once after listen registration complete
 */
Dronos.prototype.listen = function( schedule, run, done ) {

	done = noop( done );

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

/**
 * Start schedule execution runner
 *
 * When a Dronos instance is started, it will begin firing events for all active, registered schedules as often as once per minute.
 */
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

		async.series( [
			function( done ) {
				self._runReadySchedules( done );
			}
		], function() {

			// if no longer running, nothing to do
			if ( self._running === false ) {
				return;
			}

			var now = moment();
			// calculate offset to wait until top of next minute
			var nextRun = moment().add( 1, 'minute' ).millisecond( 0 ).seconds( 0 );
			var offset = nextRun.valueOf() - now.valueOf();

			if ( offset < 0 ) {
				offset *= -1;
			}
			if ( offset < 1000 ) {
				offset = 60000 - offset;
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

/**
 * Stop schedule execution runner
 *
 * Stops a Dronos instance from firing dronos events.
 */
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
			enabled:  true,
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

				var nextRun = getNextRun( schedule );

				// before attempting to run each schedule, see if we are shutting down
				if ( self._running === false || !nextRun ) {
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
							_nextRun: nextRun.toISOString()
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
	runner.call( schedule, {
		owner:  schedule.owner,
		name:   schedule.name,
		params: schedule.params
	}, function() {
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

	return [ 'owner', 'name' ].every( function( field ) {
		var result = schedule.hasOwnProperty( field ) &&
					 typeof schedule[ field ] === 'string' &&
					 schedule[ field ].length > 0;
		if ( !result ) {
			done( field + ' is a required string parameter' );
		}
		return result;
	} );

}

function validateScheduleBasicFields( schedule, done ) {

	done = nonoop( done );

	return _validateScheduleBasicFields( schedule, function( err ) {
		if ( err ) {
			done( new Error( err ) );
		} else {
			done( null );
		}
	} );
}

function _validateSchedule( schedule, done ) {

	done = nonoop( done );

	if ( !_validateScheduleBasicFields( schedule, done ) ) {
		return false;
	}

	// optional boolean fields
	if ( ![ 'enabled' ].every( function( field ) {

			if ( schedule.hasOwnProperty( field ) ) {
				return typeof schedule[ field ] === 'boolean';
			}

			return true;

		} ) ) {
		return false;
	}

	// required, non-empty strings
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

	// optional dates
	return [ 'start', 'end' ].every( function( field ) {

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

	} );

}

function getNextRun( schedule ) {

	var s = later.schedule( later.parse.cron( schedule.recurrence ) );

	var args = [ 1 ];

	var now = moment();

	// If the schedule has a start time, ensure the next run time is after that start time
	if ( schedule.hasOwnProperty( 'start' ) && moment( schedule.start ).isAfter( now ) ) {
		args.push( schedule.start );
	} else { // The later module has a bug where it will calculate next to be now if there are < 1 second on the current time stamp
		args.push( now.add( 1, 'second' ).toISOString() );
	}

	// gets the next run time of the schedule and clamps milli/seconds to zero
	var nextRun = moment( s.next.apply( s, args ).toISOString() ).millisecond( 0 ).seconds( 0 );

	// if schedule has an end time, and next time is beyond that point, cancel the next run
	if ( schedule.hasOwnProperty( 'end' ) && moment( schedule.end ).isBefore( nextRun ) ) {
		return null;
	}

	return nextRun;
}

function noop( cb ) {

	if ( typeof cb === 'function' ) {
		return cb;
	}

	return function() {
		// NO-OP	
	};

}

function nonoop( cb ) {

	if ( typeof cb === 'function' ) {
		return cb;
	}

	throw new Error( 'callback must be a function' );

}