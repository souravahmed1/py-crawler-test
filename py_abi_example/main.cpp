#include <Python.h>

void call_python_callback(PyObject* callback, PyObject* data)
{
    PyObject* result = PyObject_CallFunctionObjArgs(callback, data, NULL);
    if (result == NULL) {
        // handle error
    }
    Py_DECREF(result);
}

void my_cpp_function(PyObject* callback)
{
    // Create a Python list of dictionaries
    PyObject* data = PyList_New(2);
    PyObject* dict1 = PyDict_New();
    PyObject* dict2 = PyDict_New();
    PyDict_SetItemString(dict1, "key1", PyUnicode_FromString("value1"));
    PyDict_SetItemString(dict1, "key2", PyUnicode_FromString("value2"));
    PyDict_SetItemString(dict2, "key1", PyUnicode_FromString("value3"));
    PyDict_SetItemString(dict2, "key2", PyUnicode_FromString("value4"));
    PyList_SetItem(data, 0, dict1);
    PyList_SetItem(data, 1, dict2);

    // Call the Python callback function
    call_python_callback(callback, data);

    Py_DECREF(data);
}

void call_python_callback(PyObject* callback, PyObject* data);
void my_cpp_function(PyObject* callback);

int main(int argc, char** argv)
{
    Py_Initialize();

    // Get a reference to the Python callback function
    PyObject* module = PyImport_ImportModule("my_module");
    PyObject* callback = PyObject_GetAttrString(module, "my_callback");

    // Call the C++ function
    my_cpp_function(callback);

    Py_DECREF(callback);
    Py_DECREF(module);

    Py_Finalize();
    return 0;
}