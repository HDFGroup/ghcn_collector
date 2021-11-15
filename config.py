
import os
import sys
import yaml

cfg = {}

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def debug(*args, **kwargs):
    # can't use log.debug since that calls back to cfg
    if "LOG_LEVEL" in os.environ and os.environ["LOG_LEVEL"] == "DEBUG":
        print("DEBUG>", *args, **kwargs)


def _has_unit(cfgval):
    """ return True if val has unit char at end of string,
        otherwise return False
    """
    if isinstance(cfgval, str):
        if len(cfgval) > 1 and cfgval[-1] in ('g', 'm', 'k'):
            if cfgval[:-1].isdigit():
                return True
    return False


def getCmdLineArg(x):
    # return value of command-line option
    # use "--x=val" to set option 'x' to 'val'
    # use "--x" for boolean flags
    option = '--'+x+'='
    for i in range(1, len(sys.argv)):
        arg = sys.argv[i]
        if arg == '--'+x:
            # boolean flag
            debug(f"got cmd line flag for {x}")
            return True
        elif arg.startswith(option):
            # found an override
            override = arg[len(option):]  # return text after option string
            debug(f"got cmd line override for {x}")
            return override
    return None


def _load_cfg():
    # load config yaml
    yml_file = None
    yml_override = None
    override_yml_filepath = None
    config_dirs = []
    # check if there is a command line option for config directory
    config_dir = getCmdLineArg("config-dir")
    if config_dir:
        config_dirs.append(config_dir)
    if not config_dirs and "CONFIG_DIR" in os.environ:
        config_dirs.append(os.environ["CONFIG_DIR"])
        debug(f"got environment override for config-dir: {config_dirs[0]}")
    if not config_dirs:
        config_dirs = ["/config", "."]  # default locations
    for config_dir in config_dirs:
        file_name = os.path.join(config_dir, "config.yml")
        debug("checking config path:", file_name)
        if os.path.isfile(file_name):
            yml_file = file_name
        override_name = os.path.join(config_dir, "override.yml")
        debug("checking config path:", override_name)
        if os.path.isfile(override_name):
            override_yml_filepath = override_name
    if not yml_file:
        msg = f"config.yml not found in config_dir: {config_dirs}"
        eprint(msg)
        raise FileNotFoundError(msg)
    debug(f"_load_cfg with '{yml_file}'")
    try:
        with open(yml_file, "r") as f:
            yml_config = yaml.safe_load(f)
    except yaml.scanner.ScannerError as se:
        msg = f"Error parsing config.yml: {se}"
        eprint(msg)
        raise KeyError(msg)

    # load override yaml
    if override_yml_filepath:
        debug(f"loading override configuation: {override_yml_filepath}")
        try:
            with open(override_yml_filepath, "r") as f:
                yml_override = yaml.safe_load(f)
        except yaml.scanner.ScannerError as se:
            msg = f"Error parsing '{override_yml_filepath}': {se}"
            eprint(msg)
            raise KeyError(msg)

    # apply overrides for each key and store in cfg global
    for x in yml_config:
        cfgval = yml_config[x]
        # see if there is a command-line override
        override = getCmdLineArg(x)

        # see if there are an environment variable override
        if override is None and x.upper() in os.environ:
            override = os.environ[x.upper()]
            debug(f"got env value override for {x} ")

        # see if there is a yml override
        if override is None and yml_override and x in yml_override:
            override = yml_override[x]
            debug(f"got config override for {x}")

        if override is not None:
            if cfgval is not None:
                try:
                    # convert to same type as yaml
                    override = type(cfgval)(override)
                except ValueError as ve:
                    msg = "Error applying command line override value for "
                    msg += f"key: {x}: {ve}"
                    eprint(msg)
                    # raise KeyError(msg)
            cfgval = override  # replace the yml value

        if _has_unit(cfgval):
            # convert values like 512m to corresponding integer
            u = cfgval[-1]
            n = int(cfgval[:-1])
            if u == 'k':
                cfgval = n * 1024
            elif u == 'm':
                cfgval = n * 1024*1024
            elif u == 'g':
                cfgval = n * 1024*1024*1024
            else:
                raise ValueError("Unexpected unit char")
        cfg[x] = cfgval


def get(x, default=None):
    """ get x if found in config
        otherwise return default
    """
    if not cfg:
        _load_cfg()
    if x not in cfg:
        if default is not None:
            cfg[x] = default
        else:
            raise KeyError(f"config value {x} not found")
    return cfg[x]
