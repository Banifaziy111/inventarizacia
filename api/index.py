# Точка входа для Vercel: все запросы направляются сюда.
import sys
from pathlib import Path

# Корень проекта (родитель api/)
root = Path(__file__).resolve().parent.parent
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

from app import app

# Vercel ищет объект app (Flask WSGI)
