var kue = require('kue'),
    _ = require('lodash'),
    Promise = require('bluebird');

function Buuus(queuestr/* , Actor, args */) {
  if (!(this instanceof Buuus))
    return new Buuus(queuestr, Actor);

  this._queuestr = queuestr;
  this._queue = kue.createQueue({
    redis: {
      port: process.env.REDIS_PORT_6379_TCP_PORT || process.env.REDIS_PORT || 6379,
      host: process.env.REDIS_PORT_6379_TCP_ADDR || process.env.REDIS_ADDR || 'localhost'
    }
  });

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
    job.on('complete', function(data) {
      console.log('Queue: ', this._queuestr, ' finished task:', data);

      if (data.error)
        return reject(data.error);

      resolve(data);
    }.bind(this));

    job.on('failed', function(err) {
      console.log('Queue: ', this._queuestr, ' raised an error:', err);
      reject(err);
    }.bind(this));
  }.bind(this));
};

Buuus.prototype.listen = function(queue, callback) {
  if (!this._actor) {
    throw new Error("Actor is not defined, worker mode is disabled");
  }

  if (typeof queue === 'function') {
    callback = queue;
    queue = null;
  }

  if (queue)
    this.setQueue(queue);

  if (!this._queuestr)
    throw new Error('Queue to listen is not defined');

  this._queue.process(this._queuestr, function(job, done) {
    var fn = this._actor[job.data.action],
        message = job.data.description ? job.data.description : 'another search query',
        timeindex = 'job - ' + message + ' ' + Date.now();

    if (!fn)
      return done(new Error('Unknown action requested'));

    console.log('New job:', message);

    /* for some requests type of job is not required and could be omitted
     * TODO: this case should be refactored, e.g move type attribute to data
     */

    if (!job.data.type && job.data.data)
      job.data.type = job.data.data;
    console.time(timeindex);

    fn.call(this._actor, job.data.type, job.data.data).then(function(res) {
      console.log('success');
      done(null, res);
    })
    .catch(function(err) {
      console.log('failed');
      console.log(err);
      console.trace(err);
      done(null, { error: err });
    })
    .finally(function() {
      console.timeEnd(timeindex);
    });

  }.bind(this));

  this._queue.on('error', function(err) {
    console.log('got error:', err);
  });

  process.nextTick(function() {
    callback();
  })
};

Buuus.prototype.setActor = function(Actor /*, ... */) {
  if (this._actor)
    throw new Error('Actor is already defined');

  if (!Actor)
    throw new Error('No actor provided');

  if (Actor instanceof Function) {
    var args = Array.prototype.slice.call(arguments, 1);
    this._actor = Actor.apply({}, args);
  } else {
    this._actor = Actor
  }
}

Buuus.prototype.setQueue = function(queuestr) {
  this._queuestr = queuestr;
}

module.exports = Buuus;
