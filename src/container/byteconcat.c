#include <Python.h>

static PyObject *concat_bytes(PyObject *self, PyObject *args) {
    PyObject *byte_list;  // 输入的字节列表
    char *result;         // 拼接后的字节数据
    Py_ssize_t total_len = 0;
    Py_ssize_t i = 0;

    // 解析传递给函数的参数
    if (!PyArg_ParseTuple(args, "O", &byte_list)) {
        return NULL;
    }

    if (!PyList_Check(byte_list)) {
        PyErr_SetString(PyExc_TypeError, "Argument must be a list of bytes objects.");
        return NULL;
    }

    // 计算总字节数
    Py_ssize_t list_size = PyList_Size(byte_list);
    for (i = 0; i < list_size; i++) {
        PyObject *item = PyList_GetItem(byte_list, i);
        if (!PyBytes_Check(item)) {
            PyErr_SetString(PyExc_TypeError, "List must contain bytes objects.");
            return NULL;
        }
        total_len += PyBytes_Size(item);
    }

    // 分配内存
    result = (char *)malloc(total_len);
    if (!result) {
        PyErr_SetString(PyExc_MemoryError, "Memory allocation failed.");
        return NULL;
    }

    // 拷贝字节数据
    Py_ssize_t offset = 0;
    for (i = 0; i < list_size; i++) {
        PyObject *item = PyList_GetItem(byte_list, i);
        char *bytes_data = PyBytes_AsString(item);
        Py_ssize_t bytes_len = PyBytes_Size(item);
        memcpy(result + offset, bytes_data, bytes_len);
        offset += bytes_len;
    }

    // 创建Python字节对象
    PyObject *output_bytes = PyBytes_FromStringAndSize(result, total_len);

    // 释放内存
    free(result);

    return output_bytes;
}

// 模块方法定义
static PyMethodDef methods[] = {
    {"concat_bytes", concat_bytes, METH_VARARGS, "Concatenate a list of bytes objects into one bytes object."},
    {NULL, NULL, 0, NULL}
};

// 模块定义
static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "byteconcat",
    NULL,
    -1,
    methods
};

// 模块初始化函数
PyMODINIT_FUNC PyInit_byteconcat(void) {
    return PyModule_Create(&module);
}
