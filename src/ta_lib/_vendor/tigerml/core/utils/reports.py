import os


def get_extension_of_path(path):
    _, ext = os.path.splitext(os.path.basename(path))
    return ext


def append_file_to_path(path, file_name):
    if get_extension_of_path(path):
        return path
    else:
        return os.path.join(path, file_name)
