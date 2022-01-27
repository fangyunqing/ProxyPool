import importlib


def get_cls(module_name):

    """
    加载模块
    :param module_name:
    :return:
    """

    try:
        name_list = module_name.split(".")
        name_len = len(name_list)
        if name_len > 1:
            package = importlib.import_module(".".join(name_list[:-1]), package=name_list[-2])
            return getattr(package, name_list[-1])
        else:
            return None
    except (AttributeError, TypeError):
        return None

