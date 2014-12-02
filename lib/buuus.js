var kue = require('kue'),
    _ = require('lodash'),
    Promise = require('bluebird');

function Buuus(queuestr, Actor) {
  if (!(this instanceof Buuus))
    return new Buuus(queuestr, Actor);

  this._queuestr = queuestr;
  this._queue = kue.createQueue();

  if (Actor)
    this._actor = new Actor()
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
    job.on('failed', reject);
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
    .catch(function(err) {
      console.log('Got error:', err)
      done(err);
    });
  }.bind(this));

  callback();
};

module.exports = Buuus;

