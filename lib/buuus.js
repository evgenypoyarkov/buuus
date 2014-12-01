var kue = require('kue'),
    _ = require('lodash'),
    Promise = require('bluebird');

function Buus(queue) {
  if (!(this instanceof Buus))
    return new Buus(name)

  this._queuestr = queue;
  this._queue = kue.createQueue();
  this._handlers = {};
}

Buus.prototype.addHandler = function(name, Actor, options) {
  var handler = this._handlers[name];

  if (handler)
    throw new Error('Handler named ' + name + ' is already registered');

  this._handlers[name] = new Actor(options);
};

Buus.prototype.emit = function(action, type, body, description) {
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

  var job = this._queue.create(query)
    .priority('high')
    .removeOnComplete(true)
    .save();

  return new Promise(function(resolve, reject) {
    job.on('complete', resolve);
    job.on('failed', reject);
  });
};

Buus.prototype.listen = function() {
  this._queue.process(this._queuestr, function(job, done) {
    var handler = this._handlers[job.data.name],
        message = job.data.description ? job.data.description : 'another search query';

    if (!handler)
      return done(new Error('Unknown handler requested'));

    var fn = handler[job.data.action];

    if (!fn)
      return done(new Error('Unknown action requested'));

    console.log('New job:', message);

    fn.call(handler, job.data.type, job.data.data).then(function(res) {
      console.log('RES:', res);
      done(null, res);
    })
    .catch(function(err) {
      console.log('Got error:', err)
      done(err);
    });
  });
};

module.exports = Buus;