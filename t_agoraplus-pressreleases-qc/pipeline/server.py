import cherrypy
import subprocess
import os
from datetime import datetime


class PipelineError(Exception):
    pass


class Pipeline:
    def __init__(self, commands):
        self.commands = commands
        self.process = None
        self.start_time = None
        self.stdout = []
        self.stderr = []

    def run(self, env_vars):
        if self.process is not None:
            raise PipelineError("Process was already run or is still running")
        self.start_time = datetime.now()

        # create a new environment
        self.env = os.environ.copy()

        # extract PATH if it exists
        path = env_vars.get("PATH", None)
        env_vars.pop("PATH", None)
        if path is not None:
            self.env["PATH"] = path + ":" + self.env["PATH"]

        # add the rest of the environment variables
        for key, value in env_vars.items():
            self.env[key] = value

        # run the commands
        self.process = subprocess.Popen(
            self.commands,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def get_stdout(self):
        if self.process is not None:
            self.stdout += self.process.stdout.readlines()
            return self.stdout
        return None

    def get_stderr(self):
        if self.process is not None:
            self.stderr += self.process.stderr.readlines()
            return self.stderr
        return None

    def get_return_code(self):
        if self.process is not None:
            return self.process.poll()
        return None

    def is_running(self):
        if self.process is not None and self.process.poll() is None:
            return True
        return False


class PipelineServer(object):
    current_pipeline = None
    last_pipelines = []
    max_pipeline_logs = 10

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def status(self):
        if self.current_pipeline is None:
            return {
                "status": "idle",
                "history": len(self.last_pipelines),
            }

        if self.current_pipeline.is_running():
            return {
                "status": "running",
                "start_time": self.current_pipeline.start_time.isoformat(),
                "commands": self.current_pipeline.commands,
                "history": len(self.last_pipelines),
            }

        error_code = self.current_pipeline.get_return_code()
        if error_code is not None:
            status = "success" if error_code == 0 else "error"
            return {
                "status": status,
                "start_time": self.current_pipeline.start_time.isoformat(),
                "commands": self.current_pipeline.commands,
                "stdout": self.current_pipeline.get_stdout(),
                "stderr": self.current_pipeline.get_stderr(),
                "return_code": self.current_pipeline.get_return_code(),
                "history": len(self.last_pipelines),
            }

        raise PipelineError("Unhandled server error during status check.")

    @cherrypy.expose
    @cherrypy.tools.json_out()
    @cherrypy.tools.json_in()
    def run(self):
        status = self.status()
        if self.current_pipeline is not None:
            if self.current_pipeline.is_running():
                raise cherrypy.HTTPError(400, "Pipeline is already running")
            self.last_pipelines.append(self.current_pipeline)
            if len(self.last_pipelines) > self.max_pipeline_logs:
                self.last_pipelines.pop(0)

        commands = cherrypy.request.json["commands"]
        env_vars = cherrypy.request.json["env_vars"]
        self.current_pipeline = Pipeline(
            commands=commands,
        )
        self.current_pipeline.run(env_vars)

        return {
            "status": "starting",
            "start_time": self.current_pipeline.start_time.isoformat(),
            "commands": self.current_pipeline.commands,
            "history": len(self.last_pipelines),
            "last_status": status,
        }

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def history(self, id=None):
        if id is None:
            result = {
                "history": len(self.last_pipelines),
            }
            for pipeline in self.last_pipelines:
                result[pipeline.start_time.isoformat()] = {
                    "commands": pipeline.commands,
                    "stdout": pipeline.get_stdout(),
                    "stderr": pipeline.get_stderr(),
                    "return_code": pipeline.get_return_code(),
                }
            return result
        else:
            try:
                pipeline = self.last_pipelines[int(id)]
                return {
                    "commands": pipeline.commands,
                    "stdout": pipeline.get_stdout(),
                    "stderr": pipeline.get_stderr(),
                    "return_code": pipeline.get_return_code(),
                }
            except IndexError:
                raise cherrypy.HTTPError(404, "Pipeline not found")

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def healthcheck(self):
        return "ok"


if __name__ == "__main__":
    cherrypy.config.update(
        {"server.socket_host": "0.0.0.0", "server.socket_port": 8080}
    )
    cherrypy.quickstart(PipelineServer(), "/")
