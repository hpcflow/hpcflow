import zmq

from hpcflow.sdk.execution.protocol import AbortRequest, TimeItRequest


class JobscriptClient:

    @classmethod
    def send_abort(
        cls,
        hostname: str,
        port_number: int,
        run_id: int,
        timeout_ms: int = 5000,
    ):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)

        try:
            socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
            socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
            socket.setsockopt(zmq.LINGER, 0)
            print(f"JobscriptClient connecting to tcp://{hostname}:{port_number}")
            socket.connect(f"tcp://{hostname}:{port_number}")
            print("JobscriptClient sending abort")
            socket.send_json(AbortRequest(run_id).to_dict())
            print("JobscriptClient waiting for response")
            response = socket.recv_json()
            print(f"JobscriptClient received response: {response!r}")
            return response

        finally:
            socket.close()
            context.term()

    @classmethod
    def send_timeit(
        cls,
        hostname: str,
        port_number: int,
        run_id: int,
        orchestration_time: float | None = None,
        work_time: float | None = None,
        timeout_ms: int = 5000,
    ):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)

        try:
            socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
            socket.setsockopt(zmq.SNDTIMEO, timeout_ms)
            socket.setsockopt(zmq.LINGER, 0)

            socket.connect(f"tcp://{hostname}:{port_number}")
            socket.send_json(
                TimeItRequest(
                    run_id=run_id,
                    orchestration_time=orchestration_time,
                    work_time=work_time,
                ).to_dict()
            )
            return socket.recv_json()

        finally:
            socket.close()
            context.term()
