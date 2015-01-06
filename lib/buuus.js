var kue = require('kue'),
    _ = require('lodash'),
    Promise = require('bluebird');

function Buuus(queuestr/* , Actor, args */) {
  if (!(this instanceof Buuus))
    return new Buuus(queuestr, Actor);

  this._queuestr = queuestr;
  this._queue = kue.createQueue();

  if (arguments.length >= 2) {
    var args = Array.prototype.slice.call(arguments, 1);
    this.setActor.apply(this, args);
  }
}

Buuus.prototype.emit = function(action, type, body, description) {
  var query;

  if (_.isPlainObject(action)) {
    query = action;
  } else {
    query = {
      action: action,
      type: type,
      body: body,
      description: description
    };
  }

  var job = this._queue.create(this._queuestr, query)
    .priority('high')
    .removeOnComplete(true)
    .save();

  return new Promise(function(resolve, reject) {
    job.on('complete', resolve);
    job.on('failed', function(err) {
      console.log('Rejecting:', err);
      reject(new Error(err));
    })
  });
};

Buuus.prototype.listen = function(callback) {
  if (!this._actor) {
    throw new Error("Actor is not defined, worker mode is disabled");
  }

  this._queue.process(this._queuestr, function(job, done) {
    var fn = this._actor[job.data.action],
        message = job.data.description ? job.data.description : 'another search query';

    if (!fn)
      return done(new Error('Unknown action requested'));

    console.log('New job:', message);

    fn.call(this._actor, job.data.type, job.data.data).then(function(res) {
      done(null, res);
    })
    .catch(done);
  }.bind(this));

  callback();
};

Buuus.prototype.setActor = function(Actor /*, ... */) {
  if (this._actor)
    throw new Error('Actor is already defined');

  if (!Actor)
    throw new Error('No actor provided');

  var args = Array.prototype.slice.call(arguments, 1);
  this._actor = Actor.apply({}, args);
}

module.exports = Buuus;

