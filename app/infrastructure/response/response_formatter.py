from typing import Any, Dict, Optional, Union, List


def standard_response(
    data: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = None,
    code: int = 200,
    msg: str = "操作成功",
) -> Dict[str, Any]:
    """
    创建标准的响应格式

    参数:
        data: 响应数据，可以是任何类型
        code: 响应状态码，默认200表示成功
        msg: 响应消息

    返回:
        Dict[str, Any]: 标准格式的响应对象
    """
    return {
        "code": code,
        "data": data,
        "msg": msg,
    }


def success_response(
    data: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = None,
    msg: str = "操作成功",
) -> Dict[str, Any]:
    """
    创建成功响应

    参数:
        data: 响应数据
        msg: 成功消息

    返回:
        Dict[str, Any]: 标准格式的成功响应
    """
    return standard_response(data=data, code=200, msg=msg)


def error_response(
    msg: str = "操作失败",
    code: int = 400,
    data: Optional[Union[Dict[str, Any], List[Any], str]] = None,
) -> Dict[str, Any]:
    """
    创建错误响应

    参数:
        msg: 错误消息
        code: 错误状态码，默认400表示客户端错误
        data: 可选的错误详情数据

    返回:
        Dict[str, Any]: 标准格式的错误响应
    """
    return standard_response(data=data, code=code, msg=msg)


def not_found_response(entity: str = "资源") -> Dict[str, Any]:
    """
    创建资源未找到响应

    参数:
        entity: 未找到的实体类型名称

    返回:
        Dict[str, Any]: 标准格式的404响应
    """
    return error_response(msg=f"{entity}未找到", code=404)


def unauthorized_response(msg: str = "未授权访问") -> Dict[str, Any]:
    """
    创建未授权响应

    参数:
        msg: 未授权错误消息

    返回:
        Dict[str, Any]: 标准格式的401响应
    """
    return error_response(msg=msg, code=401)
