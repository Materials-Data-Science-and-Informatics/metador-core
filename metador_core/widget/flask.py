"""Flask API to access a collection of records."""

import io
import socket
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Dict, List, Optional
from uuid import UUID

import h5py
from flask import Flask, request, send_file

from ..container import MetadorContainer
from ..ih5.container import IH5Record
from ..schema.common import FileMeta

# ----
# These functions run inside other process:

_records: Dict[str, List[str]] = {}


def update(record_uuid: str, files: List[str]):
    """(Un)register a record with the files it consists of.

    If passed `files` is empty, will unset if record is known.
    If both arguments are empty, will remove all records from list.
    If both arguments non-empty, will create or overwrite filelist for a record.

    Should only include the HDF5 / IH5 files, not any manifests or other files.
    """
    global _records
    if not record_uuid:
        if files:
            return  # invalid
        else:
            _records = {}  # clear all
    if record_uuid in _records and not files:
        del _records[record_uuid]  # remove entry
    _records[record_uuid] = list(files)  # set entry


def open_container(record_uuid: str) -> MetadorContainer:
    """Return an open metador container by uuid."""
    files = _records.get(record_uuid)
    if not files:
        raise ValueError(f"Unknown record: {record_uuid}")
    use_h5 = len(files) == 1 and str(files[0]).endswith(
        (".h5", ".hdf5", ".H5", ".HDF5")
    )
    if use_h5:
        f = h5py.File(files[0], "r")
    else:
        f = IH5Record._open(list(map(Path, files)))
    return MetadorContainer(f)


app = Flask("metador-container-data")


@app.route("/")
def index():
    return _records


@app.route("/get/<record_uuid>/<path:record_path>")
def download_binary(record_uuid, record_path):
    with open_container(record_uuid) as container:
        if record_path not in container:
            raise ValueError(f"Path not in record: {record_path}")
        obj = container[record_path][()]
        if not isinstance(obj, bytes):
            raise ValueError(f"Path not a binary object: {record_path}")

        dl = bool(request.args.get("download", False))  # as explicit file download?
        # if object has attached file metadata, use it to serve:
        filemeta = container[record_path].meta.get("common_file", FileMeta)
        def_name = f"{record_uuid}_{record_path.replace('/', '__')}"
        name = filemeta.filename if filemeta else def_name
        mime = filemeta.mimetype if filemeta else None
        return send_file(
            io.BytesIO(obj), download_name=name, mimetype=mime, as_attachment=dl
        )


def run_app(host, port, cmd_queue):
    from threading import Thread

    def listen_cmd_queue():
        while True:
            update(*cmd_queue.get())

    t = Thread(target=listen_cmd_queue)
    t.start()
    app.run(host=host, port=port)


# ----
# Ad-hoc process runner for flask API when not used as a sub-API

host: str = "127.0.0.1"
port: int = -1
_cmd: Queue = Queue()
_cnt: int = 0
_p: Optional[Process] = None

# ----
# Start/Stop flask app in separate process:


def running() -> bool:
    return _p is not None


def start():
    global _p, _cmd, port
    if _p is not None:
        raise ValueError("Metador container file Flask API already running!")

    # get a free port and use it (no way to retrieve it when letting flask choose)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((host, 0))
    port = sock.getsockname()[1]
    sock.close()

    _cmd = Queue()
    _p = Process(
        target=run_app,
        args=(
            host,
            port,
            _cmd,
        ),
    )
    _p.start()


def stop():
    global _p, port
    if _p is None:
        raise ValueError("Metador container file Flask API not running!")

    _p.terminate()
    _p.join()
    _p = None
    port = -1


def register(record_uuid: UUID, record_files: List[Path]):
    global _cnt
    _cmd.put((str(record_uuid), map(str, record_files)))
    _cnt += 1


def unregister(record_uuid: UUID):
    global _cnt
    _cnt -= 1
