from celery import Celery

app = Celery()

app.config_from_object("celeryconfig")


if __name__ == "__main__":
    result = app.send_task(
        # TODO: change run_pipeline to run.run_<my_pipeline_name>
        # (as in run.py, but with "run." as prefix)
        "run.task1",
        args=[],
        kwargs={
            "HUB3_URL": "https://clhub.clessn.cloud/",
            "HUB3_USERNAME": "patrick.poncet",
            "HUB3_PASSWORD": "w2UpJGqU5QPstpkk",
        },
        # TODO: change my-queue to <my-pipeline-name>
        queue="l_pressreleases_qc",
    )

    while not result.ready():
        pass

    print(result.result)
