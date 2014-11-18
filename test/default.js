"use strict";

var __ = require( 'doublescore' );
var assert = require( 'assert' );
var async = require( 'async' );
var Dronos = require( '../index' );
var moment = require( 'moment' );

describe( 'Dronos', function() {

	var dronos = null;

	describe( 'general', function() {

		it( 'should NOT instantiate', function() {
			assert.throws( function() {
				dronos = new Dronos();
			} );
		}, Error );

		it( 'should NOT instantiate', function() {
			assert.throws( function() {
				dronos = new Dronos( {
										 mongodb: null
									 } );
			}, Error );
		} );

		it( 'should NOT instantiate', function() {
			assert.throws( function() {
				dronos = new Dronos( {
										 mongodb: ''
									 } );
			}, Error );
		} );

		it( 'should NOT instantiate', function() {
			assert.throws( function() {
				dronos = new Dronos( {
										 mongodb: undefined
									 } );
			}, Error );
		} );

		it( 'should instantiate', function( done ) {
			dronos = new Dronos( {
									 prefix:  '_testing',
									 mongodb: 'mongodb://localhost/testing'
								 } );
			done();
		} );

	} );

	describe( 'actions', function() {

		beforeEach( function( done ) {

			dronos = new Dronos( {
									 prefix:  '_testing',
									 mongodb: 'mongodb://localhost/testing'
								 } );

			dronos.remove( {
							   owner: '1234',
							   name:  'a.test.schedule'
						   }, function( err ) {
				done( err );
			} );

		} );

		afterEach( function( done ) {

			dronos.stop();
			dronos.remove( {
							   owner: '1234',
							   name:  'a.test.schedule'
						   }, function( err ) {
				done( err );
			} );

			dronos = null;

		} );

		it( 'should NOT set a schedule item with no schedule object', function() {

			assert.throws( function() {
				dronos.set( function( err ) {
					// NO-OP
				} );
			}, /done should be a function/ );

		} );

		it( 'should NOT set a schedule item with non-object schedule', function( done ) {

			async.eachSeries( [ null, undefined, 100, 'string', [] ], function( field, done ) {

				dronos.set( field, function( err ) {
					if ( err ) {
						done();
					} else {
						done( 'field ' + typeof field + ' failed' );
					}
				} );

			}, done );

		} );

		[ 'owner', 'name', 'recurrence' ].forEach( function( field ) {
			[ null, '', undefined ].forEach( function( value ) {

				it( 'should NOT set a schedule item with invalid ' + typeof value + ' value for required field ' + field, function( done ) {

					var schedule = {
						owner:      '1234',
						name:       'a.test.schedule',
						recurrence: '*/15 * * * *'
					};
					schedule[ field ] = value;

					dronos.set( schedule, function( err ) {
						if ( err ) {
							done();
						} else {
							done( 'field ' + field + ' failed' );
						}
					} );

				} );

			} );
		} );

		it( 'should NOT set a schedule item when end time in past', function( done ) {

			dronos.set( {
							owner:      '1234',
							name:       'a.test.schedule',
							recurrence: '*/15 * * * *',
							end:        '2013-10-01T00:00:00Z'
						}, function( err ) {
				if ( err ) {
					done();
				} else {
					done( new Error( 'did not return an error' ) );
				}
			} );

		} );

		it( 'should NOT set a schedule item when end time is before start time', function( done ) {

			dronos.set( {
							owner:      '1234',
							name:       'a.test.schedule',
							recurrence: '*/15 * * * *',
							start:      moment().add( 2, 'years' ),
							end:        moment().add( 2, 'days' )
						}, function( err ) {
				if ( err ) {
					done();
				} else {
					done(  new Error( 'did not return an error') );
				}
			} );

		} );

		it( 'should set/get a schedule item', function( done ) {

			var inputSchedule = {
				owner:      '1234',
				name:       'a.test.schedule',
				recurrence: '*/15 * * * *',
				start:      '2014-01-01T00:12:00.001Z'
			};

			dronos.set( inputSchedule, function( err ) {

				if ( err ) {
					done( err );
					return;
				}

				dronos.get( inputSchedule, function( err, schedule ) {

					if ( err ) {
						done( err );
						return;
					}

					try {

						var fields = [ 'start', 'name', 'recurrence', 'owner' ];

						for ( var i = 0; i < fields.length; i++ ) {

							if ( typeof inputSchedule[ fields[ i ] ] === 'object' && typeof inputSchedule[ fields[ i ] ].toISOString === 'function' ) {
								inputSchedule[ fields[ i ] ] = inputSchedule[ fields[ i ] ].toISOString();
							}
							if ( typeof schedule[ fields[ i ] ] === 'object' && typeof schedule[ fields[ i ] ].toISOString === 'function' ) {
								schedule[ fields[ i ] ] = schedule[ fields[ i ] ].toISOString();
							}

							assert.strictEqual( typeof schedule[ fields[ i ] ], typeof inputSchedule[ fields[ i ] ] );
							assert.strictEqual( schedule[ fields[ i ] ], inputSchedule[ fields[ i ] ] );
						}

						assert.strictEqual( schedule._nextRun.toISOString(), '2014-01-01T00:15:00.000Z' );

						done();
					} catch ( e ) {
						done( e );
					}

				} );

			} );

		} );

		it( 'should NOT set schedule that has end-time in the past', function( done ) {

			var inputSchedule = {
				owner:      '1234',
				name:       'a.test.schedule',
				recurrence: '*/15 * * * *',
				end:        '2014-01-01T00:12:00.001Z'
			};

			dronos.set( inputSchedule, function( err ) {

				try {

					assert.notEqual( err, null );

					done();
				} catch ( e ) {
					done( e );
				}

			} );

		} );

		it( 'should listenAll even if no schedules exist', function( done ) {

			var inputSchedule = {
				owner:      '1234',
				name:       'a.test.schedule',
				recurrence: '*/15 * * * *'
			};

			dronos.remove( inputSchedule, function( err ) {

				if ( err ) {
					done( err );
					return;
				}

				var run = function() {
					// NO-OP
				};
				dronos.listenAll( run );

				try {

					assert.strictEqual(
						dronos._handlers,
						run
					);

					done();
				} catch ( e ) {
					done( e );
				}

			} );

		} );

		it( 'should set/listen to a schedule item', function( done ) {

			var inputSchedule = {
				owner:      '1234',
				name:       'a.test.schedule',
				recurrence: '*/15 * * * *'
			};

			dronos.set( inputSchedule, function( err ) {

				if ( err ) {
					done( err );
					return;
				}

				var run = function() {
					// NO-OP
				};
				dronos.listen( inputSchedule, run, function( err ) {

					if ( err ) {
						done( err );
						return;
					}

					try {

						var key = JSON.stringify( {
													  owner: inputSchedule.owner,
													  name:  inputSchedule.name || null
												  } );

						assert.strictEqual(
							dronos._handlers[ key ],
							run
						);

						done();
					} catch ( e ) {
						done( e );
					}

				} );

			} );

		} );

		it( 'should NOT listen to a scheduled item with wrong run function', function( done ) {

			dronos.listen( {
							   owner: '1234',
							   name:  'a.test.schedule'
						   }, null, function( err ) {

				try {
					assert.notEqual( err, null );
					done();
				} catch ( e ) {
					done( e );
				}

			} );

		} );

		it( 'should NOT listen to a scheduled item without run function', function() {

			assert.throws( function() {
				dronos.listen( {
								   owner: '1234',
								   name:  'a.test.schedule'
							   }, function() {
					// NO-OP
				} );
			}, /run field must be a function/ );

		} );

		it( 'should remove a scheduled item, and should only return true if the item exists', function( done ) {

			var inputSchedule = {
				owner:      '1234',
				name:       'a.test.schedule',
				recurrence: '*/15 * * * *'
			};

			dronos.set( inputSchedule, function( err ) {

				if ( err ) {
					done( err );
					return;
				}

				dronos.remove( {
								   owner: '1234',
								   name:  'a.test.schedule'
							   }, function( err, removedOne ) {

					try {
						assert.ifError( err );
						assert.strictEqual( removedOne, true );

						dronos.remove( {
										   owner: '1234',
										   name:  'a.test.schedule'
									   }, function( err, removedOne ) {

							try {
								assert.ifError( err );
								assert.strictEqual( removedOne, false );
								done();
							} catch ( e ) {
								done( e );
							}

						} );

					} catch ( e ) {
						done( e );
					}

				} );
			} );

		} );

		it( 'should NOT get a scheduled item that does not exist', function( done ) {

			dronos.get( {
							owner: '1234',
							name:  'a.test.schedule'
						}, function( err, schedule ) {

				try {
					assert.strictEqual( schedule, null );
					done();
				} catch ( e ) {
					done( e );
				}

			} );

		} );

		it( 'should NOT listen to a scheduled item that does not exist', function( done ) {

			dronos.listen( {
							   owner: '1234',
							   name:  'a.test.schedule'
						   }, function() {
				// NO-OP
			}, function( err ) {

				try {
					assert.notEqual( err, null );
					done();
				} catch ( e ) {
					done( e );
				}

			} );

		} );

	} );

	describe( 'runner', function() {

		beforeEach( function( done ) {

			dronos = new Dronos( {
									 prefix:  '_testing',
									 mongodb: 'mongodb://localhost/testing'
								 } );

			dronos.remove( {
							   owner: '1234',
							   name:  'a.test.schedule'
						   },
						   function( err ) {
							   done( err );
						   } );

		} );

		afterEach( function( done ) {

			dronos.stop();
			dronos.remove( {
							   owner: '1234',
							   name:  'a.test.schedule'
						   },
						   function( err ) {
							   done( err );
						   } );
			dronos = null;

		} );

		it( 'should fire an event', function( done ) {

			var inputSchedule = {
				owner:      '1234',
				name:       'a.test.schedule',
				recurrence: '* * * * *'
			};

			dronos.set( inputSchedule, function( err ) {

				if ( err ) {
					done( err );
					return;
				}

				var runCount = 0;

				var run = function( schedule, done ) {
					runCount++;
					done();
				};
				dronos.listen( inputSchedule, run, function( err ) {

					if ( err ) {
						done( err );
						return;
					}

					try {

						var key = JSON.stringify( {
													  owner: inputSchedule.owner,
													  name:  inputSchedule.name || null
												  } );

						assert.strictEqual( dronos._handlers[ key ], run );

						dronos._models.Dronos.update( {}, { $set: { _nextRun: '2011-10-10T00:00:00Z' } }, { multi: true }, function( err, count ) {
							dronos._running = true;
							dronos._runReadySchedules( function() {
								dronos._runReadySchedules( function() {
									dronos._running = false;

									try {
										assert.strictEqual( runCount, 1 );
										done();
									} catch ( e ) {
										done( e );
									}
								} );
							} );
						} );

					} catch ( e ) {
						done( e );
					}

				} );

			} );

		} );

	} );

} );