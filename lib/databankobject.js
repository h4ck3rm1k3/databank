// databankobject.js
//
// abstraction for CRUD'ing an object as JSON to a Databank
//
// Copyright 2011, 2012 E14N https://e14n.com/
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

var Step = require('step'),
    Databank = require('./databank').Databank;

var DatabankObject = function(properties) {
    DatabankObject.init(this, properties);
};

DatabankObject.init = function(obj, properties) {
    DatabankObject.copy(obj, properties);
};

DatabankObject.copy = function(obj, properties) {
    var property;
    for (property in properties) {
        obj[property] = properties[property];
    }
};

DatabankObject.driver = null;
DatabankObject.params = {};
DatabankObject.bank = null;

// Utilities for default hooks

var passThru0 = function(callback) {
    callback(null);
};

var passThru1 = function(arg, callback) {
    callback(null, arg);
};

// Default hooks

DatabankObject.beforeCreate = passThru1;
DatabankObject.prototype.afterCreate = passThru0;

DatabankObject.beforeGet = passThru1;
DatabankObject.prototype.afterGet = passThru0;

DatabankObject.prototype.beforeUpdate = passThru1;
DatabankObject.prototype.afterUpdate = passThru0;

DatabankObject.prototype.beforeDel = passThru0;
DatabankObject.prototype.afterDel = passThru0;

DatabankObject.prototype.beforeSave = passThru0;
DatabankObject.prototype.afterSave = passThru0;

DatabankObject.subClass = function(type, Parent) {

    var Cls;

    if (!Parent) {
        Parent = DatabankObject;
    }

    Cls = function(properties) {
        Cls.init(this, properties);
    };

    Cls.init = function(inst, properties) {
        Parent.init(inst, properties);
    };

    Cls.parent = Parent;

    Cls.beforeCreate = Parent.beforeCreate || DatabankObject.beforeCreate;
    Cls.beforeGet = Parent.beforeGet || DatabankObject.beforeGet;

    Cls.bank = function() {
        return DatabankObject.bank;
    };

    Cls.type = type;

    Cls.get = function(id, callback) {
        console.log("get ID" + id)
        Cls.beforeGet(id, function(err) {
            if (err) {
                callback(err, null);
            } else {
                Cls.bank().read(Cls.type, id, function(err, value) {
                    console.log("read id:" + id)
                    console.log("read value:" + value)
                    var c;
                    if (err) {
                        callback(err, null);
                    } else {
                        c = new Cls(value);
                        console.log("read value" + c)
                        c.afterGet(function(err) {
                            if (err) {
                                callback(err, null);
                            } else {
                                callback(null, c);
                            }
                        });
                    }
                });
            }
        });
    };

    Cls.search = function(criteria, callback) {
        var results = [];

        Cls.bank().search(Cls.type,
                          criteria,
                          function(value) {
                              results.push(new Cls(value));
                          },
                          function(err) {
                              if (err) {
                                  callback(err, null);
                              } else {
                                  callback(null, results);
                              }
                          });
    };

    Cls.scan = function(each, callback) {

        Cls.bank().scan(Cls.type,
                        function(value) {
                            each(new Cls(value));
                        },
                        callback);
    };

    Cls.pkey = function() {
        var bank;
        if (Cls.schema && Cls.schema.pkey) {
            return Cls.schema.pkey;
        }
        if (Cls.schema && Cls.schema[Cls.type] && Cls.schema[Cls.type].pkey) {
            return Cls.schema[Cls.type].pkey;
        }
        bank = Cls.bank();
        if (bank && bank.schema && bank.schema[Cls.type] && bank.schema[Cls.type].pkey) {
            return bank.schema[Cls.type].pkey;
        }
        return "id";
    };

    Cls.create = function(orig, callback) {
        console.log("create" + orig)
        Cls.beforeCreate(orig, function(err, props) {
            if (err) {
                callback(err, null);
            } else {
                Cls.bank().create(Cls.type, props[Cls.pkey()], props, function(err, value) {
                    var inst;
                    if (err) {
                        callback(err, null);
                    } else {
                        console.log("create value" + value)
                        inst = new Cls(value);
                        console.log("create inst" + inst)
                        inst.afterCreate(function(err) {
                            if (err) {
                                callback(err, null);
                            } else {
                                callback(null, inst);
                            }
                        });
                    }
                });
            }
        });
    };

    Cls.readAll = function(ids, callback) {
        var clsMap = {};

        Step(
            function() {
                var i, group = this.group();
                for (i = 0; i < ids.length; i++) {
                    Cls.beforeGet(ids[i], group());
                }
            },
            function(err, results) {
                if (err) throw err;
                ids = results;
                Cls.bank().readAll(Cls.type, ids, this);
            },
            function(err, map) {
                var id, group = this.group();
                if (err) throw err;
                for (id in map) {
                    if (map.hasOwnProperty(id)) {
                        if (map[id]) {
                            clsMap[id] = new Cls(map[id]);
                            clsMap[id].afterGet(group());
                        } else {
                            clsMap[id] = null;
                            group()(null);
                        }
                    }
                }
            },
            function(err) {
                if (err) {
                    callback(err, null);
                } else {
                    callback(null, clsMap);
                }
            }
        );
    };

    Cls.readArray = function(ids, callback) {
        console.log("databank/lib/databankobject read array" + ids);
        Step(
            function() {
                Cls.readAll(ids, this);
            },
            function(err, map) {
                var i, results;
                if (err) {
                    callback(err);
                } else {
                    results = new Array();
                    for (i = 0; i < ids.length; i++) {

                        v =  ids[i];
                        if (v != '') {
                            m = map[v];
                            if (m != null) {
                                console.log("databank/lib/databankobject read array i:" + i + " v:" + v + " m:" + m);
                                results.push(m);
                            }
                            else
                            {
                                console.log("databank/lib/databankobject read NULL array i:" + i + " v:" + v + " m:" + m);
                            }
                        }
                        else
                        {
                            console.log("databank/lib/databankobject read space array i:" + i + " v:" + v );
                        }
                    }
                    callback(null, results);
                }
            }
        );
    };

    Cls.prototype = new Parent({
        update: function(orig, callback) {
            var inst = this;
            inst.beforeUpdate(orig, function(err, props) {
                if (err) {
                    callback(err, null);
                } else {
                    DatabankObject.copy(inst, props);
                    Cls.bank().update(Cls.type, inst[Cls.pkey()], inst, function(err, value) {
                        if (err) {
                            callback(err, null);
                        } else {
                            DatabankObject.copy(inst, value); // may be updated
                            inst.afterUpdate(function(err) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, inst);
                                }
                            });
                        }
                    });
                }
            });
        },
        del: function(callback) {
            var inst = this;
            inst.beforeDel(function(err) {
                if (err) {
                    callback(err);
                } else {
                    Cls.bank().del(Cls.type, inst[Cls.pkey()], function(err) {
                        if (err) {
                            callback(err);
                        } else {
                            inst.afterDel(callback);
                        }
                    });
                }
            });
        },
        save: function(callback) {
            var inst = this;
            inst.beforeSave(function(err) {
                if (err) {
                    callback(err);
                } else {
                    console.log("save inst1:" + inst[Cls.pkey()])
                    console.log("save inst:" + inst)
                    Cls.bank().save(Cls.type, inst[Cls.pkey()], inst, function(err, value) {
                        if (err) {
                            callback(err, null);
                        } else {
                            console.log("save inst:" + inst)
                            console.log("save:" + value)
                            DatabankObject.copy(inst, value); // may be updated
                            inst.afterSave(function(err) {
                                if (err) {
                                    callback(err, null);
                                } else {
                                    callback(null, inst);
                                }
                            });
                        }
                    });
                }
            });
        },
        toString: function() {
            var obj = this,
                id = obj[Cls.pkey()] || "Undefined";
            return "[" + Cls.type + " " + id + "]";
        }
    });

    return Cls;
};

exports.DatabankObject = DatabankObject;
