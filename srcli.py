# -*- coding: utf-8 -*-
# Author      : ShiFan
# Created Date: 2023/4/14 11:08
import argparse
import json
import sys
import warnings
from traceback import format_exception

import yaml
warnings.simplefilter("ignore")

from srkv.api import API


def main():
    parser = argparse.ArgumentParser(
        # usage='alter network interfaces config',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="You must run as root!",
        epilog="""
e.g.:
create key-value
    python %(prog)s create key value
update value
    python %(prog)s update key value
get value
    python %(prog)s get key
delete value
    python %(prog)s delete key
            """
    )
    sub_parser = parser.add_subparsers(
        title="SR-CLI", help="cluster info and CURD.")

    cluster_parser = sub_parser.add_parser("cluster")
    cluster_func = cluster_parser.add_subparsers(title="cluster func")
    info = cluster_func.add_parser("info", help="node info, default all nodes.")

    create_parser = sub_parser.add_parser("create")
    create_parser.add_argument("key")
    create_parser.add_argument("value", type=json.loads, help="一个json字符串")
    update_parser = sub_parser.add_parser("update")
    update_parser.add_argument("key")
    update_parser.add_argument("value", type=json.loads, help="一个json字符串")
    get_parser = sub_parser.add_parser("get")
    get_parser.add_argument("key")
    get_parser.add_argument("--prefix", action="store_true")
    delete_parser = sub_parser.add_parser("delete")
    delete_parser.add_argument("key")

    parser.add_argument('--json', action="store_true", help="以json字符串形式输出。")
    parser.add_argument('--yaml', action="store_true", help="以yaml字符串形式输出。")
    result = {"result": None, "error": None}
    sr = None
    is_json = False
    is_yaml = False
    try:
        sr = API()

        info.set_defaults(func=sr.info)
        create_parser.set_defaults(func=sr.create_kv)
        update_parser.set_defaults(func=sr.update_kv)
        get_parser.set_defaults(func=sr.get_kv)
        delete_parser.set_defaults(func=sr.delete_kv)

        args = parser.parse_args()
        is_json = args.json
        is_yaml = args.yaml
        delattr(args, "json")
        delattr(args, "yaml")
        func = getattr(args, "func")
        delattr(args, "func")
        # _std["stdout"].write(json.dumps(vars(args)))
        result["result"] = func(**vars(args))
    except Exception:
        result["error"] = "".join(format_exception(*sys.exc_info()))
    finally:
        if sr:
            sr.close()
        if is_yaml:
            print(yaml.dump(result))
        elif is_json:
            print(json.dumps(result))
        else:
            print("result:\n%s" % result["result"])
            print("error:\n%s" % result["error"])


if __name__ == "__main__":
    main()
