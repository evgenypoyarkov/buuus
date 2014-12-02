buuus
=====

A simple task/job service built on top of kue/redis

Usage:
======

As worker:

```
var Buuus = require('buuus'),
    service = new Buuus('queue-name', Actor);

service.listen(function() {
  console.log('ready');
});
```

As client:

```
var Buuus = require('buuus'),
    service = new Buuus('queue-name'),
    payload = 'Data to pass';

service.emit({
  action: 'someFunctionOfActor',
  data: payload
})
.then(function(res) {
  console.log(res);
});
```
