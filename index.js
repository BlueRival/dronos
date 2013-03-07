"use strict";

var fs = require( 'fs' );
var dronosInstance = null;

module.exports.init = function ( config ) {

	// should only run one dronos per process
	if ( dronosInstance ) {
		return dronosInstance;
	}

	config = {
		libDir: './lib',
		collectionPrefix: config.collectionPrefix || null,
		ensureEntries: config.ensureEntries || [],
		deleteEntries: config.deleteEntries || [],
		mongoose: config.mongoose || false
	};

	if ( !config.mongoose ) {
		return false;
	}

	var context = {
		options: config
	};

	if ( typeof context.options.collectionPrefix !== 'string' ) {
		context.options.collectionPrefix = "";
	}

	if ( !Array.isArray( context.options.ensureEntries ) ) {
		context.options.ensureEntries = [];
	}

	context.DronosModel = require( './models/Dronos.js' ).init( {
		mongoose: config.mongoose,
		prefix:   context.options.collectionPrefix
	} );

	if ( !context.DronosModel ) {
		return false;
	}

	dronosInstance = startUp( context );

	return dronosInstance;

};

function startUp( context ) {

	var dronos = new Dronos( context );

	dronos.ensureEntries();
	dronos.deleteEntries();

	dronos.run();

	return dronos;

}

var Dronos = function ( context ) {

	this.context = context;

};

Dronos.prototype.deleteEntries = function () {
	var entries = this.context.options.deleteEntries;

	for ( var i = 0; i < entries.length; i++ ) {
		this._deleteEntry( entries[i] );
	}
};

Dronos.prototype.ensureEntries = function () {

	var entries = this.context.options.ensureEntries;

	for ( var i = 0; i < entries.length; i++ ) {
		this.ensureEntry( entries[i] );
	}

};

Dronos.prototype.ensureEntry = function ( entry ) {

	// using serialization to clone
	var template = {
		account:           null,
		name:              null,
		recurrencePattern: {
			minute:     null,
			hour:       null,
			dayOfMonth: null,
			month:      null,
			dayOfWeek:  null
		},
		lib:               null,
		method:            null,
		processingLimit:   null
	};

	// create default by cloning template, then mixing in the entry
	entry = mixin( clone( template ), entry );

	if ( !this._verifyDataContainsRequiredFields( template, entry ) ) {
		return false;
	}

	this._ensureChronEntry( entry );

	return true;

};

Dronos.prototype._deleteEntry = function ( entry ) {

	entry = {
		account: entry.account || null,
		name: entry.name || null
	};

	if ( entry.account !== null && entry.name !== null ) {
		this.context.DronosModel.remove(
			{
				account: entry.account,
				name:    entry.name
			}
		).exec();
		return true;
	}

	return false;

};

Dronos.prototype._ensureChronEntry = function ( entry ) {

	var self = this;

	this.context.DronosModel.findOneAndUpdate(
		{
			account: entry.account,
			name:    entry.name
		},
		{
			account:           entry.account,
			name:              entry.name,
			lastUpdate:        new Date(),
			repeat:            true,
			recurrencePattern: {
				minute:     entry.recurrencePattern.minute,
				hour:       entry.recurrencePattern.hour,
				dayOfMonth: entry.recurrencePattern.dayOfMonth,
				month:      entry.recurrencePattern.month,
				dayOfWeek:  entry.recurrencePattern.dayOfWeek
			},
			lib:               entry.lib,
			method:            entry.method,
			processingLimit:   entry.processingLimit,
			params:            ( entry.params ? entry.params : null )
		},
		{
			new:    true,
			upsert: true
		},
		function ( err, data ) {
			if ( data && data._id ) {

				var threshold = new Date();
				threshold.setSeconds( threshold.getSeconds() + 15 );

				self.context.DronosModel.update(
					{
						_id: data._id,
						$or: [
							{
								nextRun: {
									$gt: threshold
								}
							},
							{
								nextRun: {
									$exists: false
								}
							}
						]
					},
					{
						nextRun:    _getNextRunTime( entry.recurrencePattern ),
						lastUpdate: new Date()
					}
				).exec();

			}
		}

	);

};

Dronos.prototype._verifyDataContainsRequiredFields = function ( template, data, path ) {

	if ( !path ) {
		path = "";
	}

	for ( var field in template ) {
		if ( template.hasOwnProperty( field ) ) {
			var val = template[field];

			// check for scalar in template
			if ( val === null ) {
				if ( typeof data[field] === 'undefined' || data[field] === null ) {
					return false;
				}
			}
			else if ( Object.isObject( val ) ) {

				// data not an object, so don't bother recurse
				if ( !Object.isObject( data[field] ) ) {
					return false;
				}

				// recurse
				if ( !this._verifyDataContainsRequiredFields( val, data[field], path + field + "." ) ) {
					return false;
				}

			}
			else {
				return false;
			}
		}
	}

	return true;
};

Dronos.prototype.run = function () {

	//console.log( process.pid, 'Dronos: start running', new Date().toISOString() );

	var self = this;

	this.context.runningIds = [];

	this._resetBrokenEntries();

	// run entries, until no more are ready for the current minute, then sleep
	this._executeEntries( function () {

		// make sure run gets called again
		self._nextRunTimeout();

	} );

};

Dronos.prototype._resetBrokenEntries = function () {

	var threshold = new Date();

	// no process should execute for more than 15 seconds, so if it still has processing count above a 30 second threshold, clear stuff
	threshold.setSeconds( threshold.getSeconds() - 45 );

	this.context.DronosModel.update(
		{
			$or: [
				{
					nextRun:         { $lt: threshold }, // if entry should have finished, but process count still high, assume node process failed to decrement
					processingCount: { $gt: 0 }
				},
				{
					lastUpdate:      { $lt: threshold }, // if entry last update was more than 30 seconds ago, but processing count still up, decrement
					processingCount: { $gt: 0 }
				},
				{
					processingCount: { $lt: 0 } // processing count should never be below 0. can't have negative number of processing working
				}
			]
		},
		{
			$set:       {
				processingCount: 0
			},
			lastUpdate: new Date()
		},
		{
			multi: true
		}
	).exec( function ( err, count ) {
			if ( count ) {
				console.log( process.pid, 'Dronos: reset broken entries', count );
			}
		} );

};

Dronos.prototype._executeEntries = function ( callback, time ) {

	var self = this;

	if ( !time ) {

		time = new Date();

		// we run right on the minute, and servers may be off in
		// time by a couple seconds, but less than 60 seconds. this
		// makes sure we aren't missing items for the current second
		// if this server's time is actually a little slow
		time.setSeconds( time.getSeconds() + 10 );

	}

	this.context.DronosModel.findOneAndUpdate(
		{
			_id:     { $nin: this.context.runningIds },
			nextRun: { $lte: time }, // must be ready to run
			$or:     [
				// must have less than max limit processes running, or unlimited processing limit
				{
					processingLimit: 0
				},
				{
					$where: 'this.processingCount < this.processingLimit'
				},
				{
					processingCount: {
						$exists: false
					}
				}
			]
		},
		{
			$inc:       { processingCount: 1 },
			lastUpdate: new Date()
		},
		{
			sort: {
				nextRun:         1, // ones waiting longest get priority
				processingCount: 1 // if tied for longest waiting, one with fewest running gets priority
			},
			new:  true
		},
		function ( err, data ) {

			// no more entries
			if ( !data ) {
				process.nextTick( callback );
				return;
			}

			data = data.toObject ? data.toObject() : {};

			var entry = {
				_id: data._id || null,
				lib: data.lib || null,
				method: data.method || null,
				params: data.params || null
			};

			// no entry, we are done
			if ( entry._id === null ) {
				process.nextTick( callback );
			}
			else {

				// make sure we don't run multiple instances of the same entry
				self.context.runningIds.push( entry._id );

				// start the entry running
				self._executeEntry( entry );

				// break up execution a little
				process.nextTick( function () {

					// if we just ran an entry, we always look for one more runable entry and forward the callback down
					self._executeEntries( callback, time );

				} );

			}

		}
	);

};

Dronos.prototype._executeEntry = function ( entry, callback ) {

	var lib = (process.cwd() + '/' + this.context.options.libDir + '/' + entry.lib).replace( /\/+/g, '/' );
	var method = entry.method;
	var params = entry.params;
	if ( !lib || !method ) {
		return;
	}
	var self = this;
	var schedulerUtils = {
		nextRunTime:   _getNextRunTime,
		incrementTime: _incrementTime
	};
	var doneCalled = false;
	var doneTimeout = null;

	var done = function () {

		if ( doneTimeout ) {
			clearTimeout( doneTimeout );
		}

		if ( doneCalled ) {
			console.error( process.pid, 'Dronos: done already called for execution of lib', entry.lib, method, new Date().toISOString() );
			return;
		}
		doneCalled = true;

		process.nextTick( function () {

			if ( entry._id ) {
				if ( entry.repeat ) {

					// build update object
					var update = {
						$inc:       { processingCount: -1 },
						lastRun:    new Date(),
						lastUpdate: new Date()
					};
					if ( entry.recurrencePattern ) {
						update.nextRun = _getNextRunTime( entry.recurrencePattern );
					}

					self.context.DronosModel.findByIdAndUpdate( entry._id, update, { new: true }, function () {
						if ( typeof callback === 'function' ) {
							process.nextTick( callback );
						}
					} );

				}
				else {

					self.context.DronosModel.findByIdAndRemove( entry._id, function () {
						if ( typeof callback === 'function' ) {
							process.nextTick( callback );
						}
					} );

				}
			}

		} );

	};

	fs.exists( lib, function ( exists ) {

		if ( !exists ) {
			console.error( 'missing lib', entry.lib );
			done();
			return;
		}

		var module = null;
		try {
			module = require( lib );
		}
		catch ( e ) {
			console.error( 'exception lib', entry.lib, e );
			done();
			return;
		}

		if ( !module || typeof module[method] !== 'function' ) {
			console.error( 'missing function', entry.lib, method );
			done();
			return;
		}

		process.nextTick( function () {
			//console.log( process.pid, 'Dronos: executing', entry.lib + "." + method, new Date().toISOString() );
			module[method]( params, done, schedulerUtils );
		} );

	} );

	// triggered event code has 15 seconds to call done, before we consider it auto-done
	doneTimeout = setTimeout( function () {
		console.error( process.pid, 'Dronos: lib.method failed to call done in 15 seconds', entry.lib, method );
		done();
	}, 15000 );

};

Dronos.prototype._nextRunTimeout = function () {

	var now = new Date();
	var offset = clone( now );
	var self = this;
	var seconds = offset.getSeconds() + 1;
	var interval = 30; // number of seconds between cron interval check

	// calculate next interval in constant time
	var secondsOffset = interval - (seconds % interval);
	if ( secondsOffset >= interval ) {
		secondsOffset = 0;
	}

	offset.setSeconds( seconds + secondsOffset );
	offset.setMilliseconds( 0 );

	this.context.runTimeout = setTimeout( function () {
		self.context.runTimeout = null;
		self.run();
	}, offset.getTime() - now.getTime() );

//	console.log( process.pid, 'Dronos: done running', new Date().toISOString() );
//	console.log( process.pid, 'Dronos: nextRun', offset.toISOString() );
};

function _getNextRunTime( pattern ) {

	return _incrementTime( new Date(), pattern );

}

function _incrementTime( time, pattern ) {

	pattern = clone( pattern );

	_parseRecurrencePattern( pattern );

	if ( pattern.dayOfMonth.type !== 'all' && pattern.dayOfWeek.type !== 'all' ) {

		var dOM = _incrementTimeDayOfMonth( time, pattern );
		var dOW = _incrementTimeDayOfWeek( time, pattern );

		if ( dOM.getMilliseconds() < dOW.getMilliseconds() ) {
			return dOM;
		}
		else {
			return dOW;
		}

	}
	else {
		return _realIncrementTime( time, pattern );
	}

}

function _incrementTimeDayOfMonth( time, pattern ) {

	pattern = clone( pattern );
	pattern.dayOfWeek = { type: 'all' };

	return _realIncrementTime( time, pattern );

}

function _incrementTimeDayOfWeek( time, pattern ) {

	pattern = clone( pattern );
	pattern.dayOfMonth = { type: 'all' };

	return _realIncrementTime( time, pattern );

}

function _realIncrementTime( time, pattern ) {

	var newTime = clone( time );

	var secondsMilliSeconds = newTime.getSeconds() + newTime.getMilliseconds();

	// we always start at the top of the minute
	newTime.setSeconds( 0 );
	newTime.setMilliseconds( 0 );

	// round up to nearest minute
	if ( secondsMilliSeconds > 0 ) {
		newTime.setMinutes( newTime.getMinutes() + 1 );
	}

	_incrementField( newTime, 'FullYear', pattern.year );
	_incrementField( newTime, 'Month', pattern.month );

	// these are mutually exclusive at this level of processing
	if ( pattern.dayOfMonth.type !== 'all' ) {
		_incrementField( newTime, 'Date', pattern.dayOfMonth );
	}
	else if ( pattern.dayOfWeek.type !== 'all' ) {
		_incrementField( newTime, 'Day', pattern.dayOfWeek );
	}

	_incrementField( newTime, 'Hours', pattern.hour );
	_incrementField( newTime, 'Minutes', pattern.minute );

	return newTime;

}

function _incrementField( time, field, pattern ) {

	var currentValue = time['get' + field]();
	var newValue = _calculateNewValue( time, field, pattern );

	// if newValue is not an increment, nothing to do
	if ( newValue <= currentValue ) {
		return;
	}

	// clamp all units of time less than the current unit
	switch ( field ) {

		case 'FullYear':
			time.setMonth( 0 );

		/* falls through */
		case 'Month':
			time.setDate( 1 );

		/* falls through */
		case 'Day': // dayOfWeek
		case 'Date': // dayOfMonth
			time.setHours( 0 );

		/* falls through */
		case 'Hours':
			time.setMinutes( 0 );

	}

	// convert day of week value to day of month
	if ( field === 'Day' ) {
		field = 'Date';
		newValue = time.getDate() + _convertDayOfWeekOffsetToDateOffset( currentValue, newValue );
	}

	// apply increment to current field
	time['set' + field]( newValue );

}

function _convertDayOfWeekOffsetToDateOffset( currentValue, newValue ) {

	// no overflow, so its just the straight distance forward from currentValue to newValue
	if ( newValue >= currentValue ) {
		return newValue - currentValue;
	}

	// we overflowed, so calculate days until early day in next week
	return 7 - currentValue + newValue;

}

function _calculateNewValue( time, field, pattern ) {

	var type = pattern.type;
	var currentValue = time['get' + field]();

	var newValue = currentValue; // default is for no value to change, such as 'all' type

	// year doesn't overflow
	if ( field === 'FullYear' ) {
		return newValue;
	}

	// determine the minimum and maximum possible values for the current field
	var min = 0;
	var max = null;
	switch ( field ) {

		case 'Month':
			max = 11;
			break;

		case 'Day': // dayOfWeek
			max = 6;
			break;

		case 'Date': // dayOfMonth
			min = 1;
			max = _lastDayOfMonth( time );
			break;

		case 'Hours':
			max = 23;
			break;

		case 'Minutes':
			max = 59;
			break;

	}

	// months are stored in Date as ints 0-11, but for humans, we think of them as jan=1, mar=3, etc. so we convert to 1-12 scale for calculations
	if ( field === 'Month' ) {
		min++;
		max++;
		currentValue++;
		newValue++;
	}

	// handle last day of the time period for given field
	if ( type === 'last' ) {
		if ( field !== 'FullYear' ) { // year doesn't support last, unless you happen to know the year the earth will end.
			newValue = max;
		}
	}

	// handle literal values
	else if ( type === 'literals' ) {
		newValue = _nextLiteral( pattern.parts, currentValue );
	}

	// handle modulus
	else if ( type === 'modulus' && field !== 'FullYear' ) {
		newValue = _nextModulus( min, max, currentValue, pattern.mod );
	}

	newValue = _handleOverflow( min, max, currentValue, newValue );

	// undo month offsets used for calculations
	if ( field === 'Month' ) {
		newValue--;
	}

	return newValue;

}

function _handleOverflow( min, max, currentValue, newValue ) {

	// no overflow
	if ( newValue >= currentValue ) {
		return newValue;
	}

	// overflow, calculate offset
	return max + newValue - min + 1;

}

function _nextLiteral( literals, current ) {

	for ( var i = 0; i < literals.length; i++ ) {
		if ( literals[i] >= current ) {
			return literals[i];
		}
	}

	return literals[0];

}

function _nextModulus( start, end, current, mod ) {

	var originalCurrent = current;

	while ( current <= end ) {
		if ( current % mod === 0 ) {
			return current;
		}
		current++;
	}
	current = start;
	while ( current < originalCurrent ) {
		if ( current % mod === 0 ) {
			return current;
		}
		current++;
	}

	return null;

}

function _lastDayOfMonth( time ) {

	var value = null;
	switch ( time.getMonth() + 1 ) { // we don't need the +1, it just makes it easier to understand the cases, since we already think of jan = 1, mar = 3, etc.

		case 1:
		case 3:
		case 5:
		case 7:
		case 8:
		case 10:
		case 12:
			value = 31;
			break;

		case 4:
		case 6:
		case 9:
		case 11:
			value = 30;
			break;

		case 2:
			value = 28 + (time.getFullYear % 4 === 0 ? 1 : 0); // account for leap year
			break;

	}

	return value;

}

function _parseRecurrencePattern( recurrencePattern ) {

	var fields = ['minute', 'hour', 'dayOfMonth', 'dayOfWeek', 'month', 'year'];

	for ( var i = 0; i < fields.length; i++ ) {
		if ( recurrencePattern.hasOwnProperty( fields[i] ) ) {
			recurrencePattern[fields[i]] = _parseRecurrencePatternEntry( recurrencePattern[fields[i]] );
		}
	}

}

function _parseRecurrencePatternEntry( entry ) {

	// used throughout function
	var matches = null;

	// check for meta "last" indicator
	if ( entry.match( /^L$/ ) ) {
		return {
			type: 'last'
		};
	}

	// check for literal exact numbers
	if ( entry.match( /^[0-9,\-]+$/ ) ) {
		var split = entry.split( /,/ );
		var parts = [];

		// handle comma separation
		for ( var i = 0; i < split.length; i++ ) {

			// handle single value
			if ( split[i].match( /^[0-9]+$/ ) ) {
				parts.push( Math.floor( split[i] ) );
			}

			// handle range
			else {

				matches = split[i].match( /^([0-9]+)-([0-9]+)$/ );
				if ( matches && matches.length > 0 ) {
					var start = Math.min( matches[1], matches[2] );
					var end = Math.max( matches[1], matches[2] );
					for ( ; start <= end; start++ ) {
						parts.push( start );
					}

				}
			}

		}

		// see if there were valid combination of numbers and commas
		if ( parts.length > 0 ) {
			return {
				type:  "literals",
				parts: parts
			};
		}

	}

	// check for modulus pattern
	matches = entry.match( /^\*\/([0-9]+)$/ );
	if ( matches && matches.length > 1 ) {

		return {
			type: "modulus",
			mod:  Math.min( Math.floor( matches[1] ), 60 ) // modulus limited to 60 because of the units where modulus makes sense (minutes, hours, dayOfMonth), 60 is the highest modulus that doesn't clamp to 0.
		};

	}

	// default to * if nothing else matches
	return {
		type: "all"
	};

}


function isObject( thing ) {
	return typeof thing === 'object' && !Array.isArray( thing ) && thing !== null;
}

function clone( thing ) {

	var target = null;
	var type = typeof thing;

	if ( thing === undefined ) {
		target = undefined;
	}
	else if ( thing === null ) {
		target = null;
	}
	else if ( type === 'string' || type === 'number' || type === 'boolean' ) {
		target = thing;
	}
	else if ( thing instanceof Date ) {
		target = new Date( thing.toISOString() );
	}
	else if ( isObject( thing ) || Array.isArray( thing ) ) {
		target = JSON.parse( JSON.stringify( thing ) ); // probably a slightly more efficient way to do thing, but thing is ok for now
	}
	else { // functions, etc. not clonable yet
		target = undefined;
	}

	return target;
}

function mixin() {

	var child = arguments[0];

	// Handle case when target is a string or something (possible in deep copy)
	if ( typeof child !== "object" && typeof child !== 'function' ) {
		child = {};
	}

	// handle arbitrary number of mixins. precedence is from last to first item passed in.
	for ( var i = 1; i < arguments.length; i++ ) {

		var parent = arguments[i];

		if ( !parent || (!Array.isArray( parent ) && !isObject( parent ) ) ) {
			continue;
		}

		// Extend the base object
		for ( var name in parent ) {

			// don't copy parent stuffs
			if ( parent.hasOwnProperty( name ) ) {

				var target = child[ name ];
				var source = parent[ name ];

				// Prevent never-ending loop
				if ( child === source ) {
					continue;
				}

				// if target exists and is an array...
				if ( Array.isArray( target ) ) {

					if ( Array.isArray( source ) ) {

						// ...merge source array into target array
						for ( var j = 0; j < source.length; j++ ) {

							if ( typeof child[ name ][j] === 'object' ) {
								child[ name ][j] = mixin( child[ name ][j], source[j] );
							}
							else {
								child[ name ][j] = source[j];
							}

						}

					}

				}
				// if target is an object, try to mixin source
				else if ( isObject( target ) ) {

					mixin( target, source );

				}
				// otherwise, target becomes source
				else {

					child[ name ] = source;

				}
			}
		}

	}

	return child;
}

