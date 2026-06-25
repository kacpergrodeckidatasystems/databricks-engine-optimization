# =========================================================================
# ZMIENNE PROJEKTOWE
# =========================================================================
VENV = venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip

.PHONY: help venv clean clean-all docker-build docker-up docker-down test package

# Domyślny target wyświetlający pomoc
help:
	@echo "========================================================================="
	@echo "📊 BESS APM AUDITOR - AUTOMATYZACJA ŚRODOWISKA LOCAL/DEV (PURE SPARK)"
	@echo "========================================================================="
	@echo "Dostępne polecenia:"
	@echo "  make venv          - Tworzy lokalny venv (Py3.11) i instaluje [dev,test]"
	@echo "  make test          - Uruchamia lokalne testy jednostkowe za pomocą pytest"
	@echo "  make package       - Buduje komercyjny artefakt dystrybucyjny (.whl)"
	@echo "  make docker-build  - Buduje zunifikowany obraz silnika PySpark od zera"
	@echo "  make docker-up     - Uruchamia kontener wykonawczy w tle"
	@echo "  make docker-down   - Zatrzymuje i usuwa kontenery dockerowe"
	@echo "  make clean         - Usuwa cache i tymczasowe pliki budowania"
	@echo "  make clean-all     - Usuwa to co 'clean' + całkowicie kasuje venv"
	@echo "========================================================================="

# =========================================================================
# 💻 LOKALNE ŚRODOWISKO WIRTUALNE (VENV)
# =========================================================================
venv:
	@echo "🚀 Tworzenie odizolowanego venv opartego o Python 3.11..."
	python3.11 -m venv $(VENV)
	@echo "⬆️ Aktualizacja pip..."
	$(PIP) install --upgrade pip
	@echo "📦 Instalacja projektu w trybie edytowalnym wraz z [dev,test]..."
	$(PIP) install -e .[dev,test]
	@echo "✅ Środowisko venv przygotowane. Aktywuj je: source venv/bin/activate"

# =========================================================================
# 🧪 TESTY JEDNOSTKOWE
# =========================================================================
test:
	@if [ ! -d "$(VENV)" ]; then echo "❌ Błąd: Brak venv. Uruchom najpierw: make venv"; exit 1; fi
	@echo "🧪 Uruchamianie testów jednostkowych wewnątrz venv..."
	$(VENV)/bin/pytest tests/ --cov=src

# =========================================================================
# 📦 BUDOWANIE ARTEFAKTU (.WHL)
# =========================================================================
package:
	@if [ ! -d "$(VENV)" ]; then echo "❌ Błąd: Brak venv. Uruchom najpierw: make venv"; exit 1; fi
	@echo "🧹 Czyszczenie starych paczek..."
	rm -rf build/ dist/ *.egg-info
	@echo "🔌 Instalacja systemowego buildera..."
	$(PIP) install --upgrade build
	@echo "📦 Kompilacja kodu do uniwersalnego Wheel (.whl) zablokowanego na Py3.11..."
	$(PYTHON) -m build
	@echo "✅ Artefakt zbudowany w katalogu dist/"

# =========================================================================
# 🐳 ORKIESTRACJA DOCKER-COMPOSE (PURE PYSPARK CORE)
# =========================================================================
docker-build:
	@echo "🐳 Budowanie zunifikowanego obrazu PySpark..."
	docker-compose build --no-cache

docker-up:
	@echo "🚀 Uruchamianie kontenera wykonawczego APM Auditor..."
	docker-compose up -d

docker-down:
	@echo "🛑 Zatrzymywanie kontenera..."
	docker-compose down

# =========================================================================
# 🧹 CZYSZCZENIE ŚRODOWISKA
# =========================================================================
clean:
	@echo "🧹 Czyszczenie plików tymczasowych, cache i logów..."
	rm -rf build/ dist/ *.egg-info .pytest_cache .coverage htmlcov
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-all: clean
	@echo "💥 Całkowite usuwanie środowiska wirtualnego venv..."
	rm -rf $(VENV)