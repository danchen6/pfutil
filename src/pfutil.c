/**
 * Copyright (c) 2024, Dan Chen <danchen666666 at gmail dot com>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#include "hyperloglog.h"

#include <Python.h>


typedef struct {
    PyObject_HEAD
    void *sds; // Redis HyperLogLog object is stored as a SDS string
} HyperLogLogObject;


static void HyperLogLog_dealloc(HyperLogLogObject *self)
{
    _pffree(self->sds);
    self->sds = NULL;
    Py_TYPE(self)->tp_free((PyObject*) self);
}


static PyObject* HyperLogLog_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    int use_dense = 0;
    static char *kwlist[] = {"use_dense", NULL};
    if (!PyArg_ParseTupleAndKeywords(args, kwds, "|$p", kwlist, &use_dense)) {
        return NULL;
    }

    HyperLogLogObject *self = (HyperLogLogObject*) PyObject_New(HyperLogLogObject, type);
    if (!self) {
        return NULL;
    }

    self->sds = _pfcreate(use_dense);
    return (PyObject*) self;
}


static PyObject* HyperLogLog_from_bytes(PyObject *cls, PyObject *args)
{
    PyObject *bytes_obj;
    if (!PyArg_ParseTuple(args, "O!", &PyBytes_Type, &bytes_obj)) {
        return NULL;
    }

    if (!PyBytes_Check(bytes_obj)) {
        PyErr_SetString(PyExc_TypeError, "Argument must be a bytes object");
        return NULL;
    }

    Py_ssize_t num_bytes = PyBytes_Size(bytes_obj);
    char const *bytes = PyBytes_AsString(bytes_obj);
    if (!bytes) {
        PyErr_SetString(PyExc_ValueError, "Failed to read from bytes");
        return NULL;
    }

    if (num_bytes < 4 || bytes[0] != 'H' || bytes[1] != 'Y' || bytes[2] != 'L' || bytes[3] != 'L') {
        PyErr_SetString(PyExc_ValueError, "Invalid signature");
        return NULL;
    }

    HyperLogLogObject *self = (HyperLogLogObject *)PyObject_New(HyperLogLogObject, (PyTypeObject *)cls);
    if (!self) {
        PyErr_SetString(PyExc_ValueError, "Failed to create HyperLogLog object");
        return NULL;
    }

    self->sds = _pfload(bytes, num_bytes);
    return (PyObject *)self;
}


static PyObject* HyperLogLog_pfadd(HyperLogLogObject *self, PyObject *args)
{
    Py_ssize_t num_args = PyTuple_Size(args);
    for (Py_ssize_t i = 0; i < num_args; i++) {
        PyObject *item = PyTuple_GetItem(args, i);
        if (!PyUnicode_Check(item)) {
            PyErr_SetString(PyExc_TypeError, "All arguments must be strings");
            return NULL;
        }

        Py_ssize_t num_bytes;
        char const *bytes = PyUnicode_AsUTF8AndSize(item, &num_bytes);
        if (pfadd(&(self->sds), bytes, num_bytes) < 0) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to add elements");
            return NULL;
        }
    }

    Py_INCREF(self);
    return (PyObject *)self;
}


static PyObject* HyperLogLog_from_elements(PyObject *cls, PyObject *args)
{
    HyperLogLogObject *self = (HyperLogLogObject*) PyObject_New(HyperLogLogObject, (PyTypeObject*) cls);
    if (!self) {
        return NULL;
    }

    int const use_dense = 0;
    self->sds = _pfcreate(use_dense);

    if (!HyperLogLog_pfadd(self, args)) {
        Py_DECREF(self);
        return NULL;
    }

    Py_DECREF(self); // HyperLogLog_pfadd() returns self
    return (PyObject *)self;
}


static PyObject* HyperLogLog_pfmerge(HyperLogLogObject *self, PyObject *args)
{
    PyObject *other_obj;

    if (!PyArg_ParseTuple(args, "O!", Py_TYPE(self), &other_obj)) {
        return NULL;
    }

    HyperLogLogObject *other = (HyperLogLogObject *)other_obj;
    int const ret = pfmerge(&(self->sds), other->sds);
    if (ret < 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to merge HyperLogLog objects");
        return NULL;
    }

    Py_INCREF(self);
    return (PyObject *)self;
}


static PyObject* HyperLogLog_pfcount(HyperLogLogObject *self, PyObject *Py_UNUSED(ignored))
{
    uint64_t const cardinality = pfcount(self->sds);
    return PyLong_FromUnsignedLongLong(cardinality);
}


static PyObject* HyperLogLog_to_bytes(HyperLogLogObject *self, PyObject *Py_UNUSED(ignored))
{
    size_t const num_bytes = _pfbytesize(self->sds);
    char const *bytes = self->sds;
    return PyBytes_FromStringAndSize(bytes, num_bytes);
}


static PyMethodDef HyperLogLog_methods[] = {
    {
        "pfadd", (PyCFunction)HyperLogLog_pfadd, METH_VARARGS,
        "Add elements to the HyperLogLog"
    },
    {
        "pfmerge", (PyCFunction)HyperLogLog_pfmerge, METH_VARARGS,
        "Merge another HyperLogLog into this one"
    },
    {
        "pfcount", (PyCFunction)HyperLogLog_pfcount, METH_NOARGS,
        "Get the cardinality"
        },
    {
        "to_bytes", (PyCFunction)HyperLogLog_to_bytes, METH_NOARGS,
        "Serialize the HyperLogLog to bytes"
        },
    {
        "from_bytes", (PyCFunction)HyperLogLog_from_bytes, METH_CLASS | METH_VARARGS,
        "Create a HyperLogLog from bytes"
        },
    {
        "from_elements", (PyCFunction)HyperLogLog_from_elements, METH_CLASS | METH_VARARGS,
        "Create a HyperLogLog from elements"
    },
    {
        NULL
    },
};


static PyTypeObject HyperLogLogType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "pfutil.HyperLogLog",
    .tp_basicsize = sizeof(HyperLogLogObject),
    .tp_itemsize = 0,
    .tp_dealloc = (destructor)HyperLogLog_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_doc = "HyperLogLog",
    .tp_methods = HyperLogLog_methods,
    .tp_new = HyperLogLog_new,
};


static PyModuleDef pfutilmodule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "pfutil",
    .m_doc = "Fast and Redis-compatible HyperLogLog extension for Python 3",
    .m_size = -1,
};


PyMODINIT_FUNC
PyInit_pfutil(void)
{
    PyObject *m;
    if (PyType_Ready(&HyperLogLogType) < 0) {
        return NULL;
    }

    m = PyModule_Create(&pfutilmodule);
    if (!m) {
        return NULL;
    }

    Py_INCREF(&HyperLogLogType);
    if (PyModule_AddObject(m, "HyperLogLog", (PyObject *)&HyperLogLogType) < 0) {
        Py_DECREF(&HyperLogLogType);
        Py_DECREF(m);
        return NULL;
    }
    return m;
}
