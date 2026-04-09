worker_class = "gthread"
threads = 4
timeout = 120


def on_starting(server):
    import subprocess
    subprocess.run(["playwright", "install", "chromium"], check=False)
