import os
from datetime import datetime as dt
from workflows import hyras, eobs, radklim_rw, hostrada
from json2args import get_parameter
from json2args.data import get_data_paths


def main():
    # Parse parameters
    kwargs = get_parameter()
    data = get_data_paths(as_dict=True)

    # Check if a toolname was set in env
    toolname = os.environ.get("TOOL_RUN", "").lower()

    # Switch the tool
    if toolname == "eobs":
        # Run the E-OBS workflow
        eobs.workflow_eobs(kwargs, data)

    elif toolname == "hyras":
        # Run the HYRAS workflow
        hyras.workflow_hyras(kwargs, data)
    elif toolname == "radklim_rw":
        # Run the RADKLIM-RW workflow
        radklim_rw.workflow_radklim_rw(kwargs, data)
    elif toolname == "hostrada":
        # Run the Hostrada workflow
        hostrada.workflow_hostrada(kwargs, data)
    else:
        # In any other case, it was not clear which tool to run
        raise AttributeError(
            f"[{dt.now().isocalendar()}] Either no TOOL_RUN environment variable available, or '{toolname}' is not valid.\n")


if __name__ == "__main__":
    main()
