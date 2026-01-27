
from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = "./templates"

# -----------------------------
# Jinja2 環境
# -----------------------------
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)

def load_template(template_name):
    return env.get_template(template_name)