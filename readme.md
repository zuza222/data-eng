## Structure

- `medallion-prefect.py` – contains all Prefect tasks and flow definition
- `Dockerfile` – builds environment with Spark + Prefect
- `requirements.txt` – Python dependencies
- `lakehouse/` – data outputs (Bronze, Silver, Gold)
- `README.md` – instructions (you're reading it!)

## 1. Setup Python environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Run Prefect server (UI + API)
```bash
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect server start
```
## 3. Register and apply deployment
Open a new terminal, activate your environment:

```bash
prefect deployment build medallion-prefect.py:medallion_flow -n "medallion-deployment" -o medallion-deployment.yaml
prefect deployment apply medallion-deployment.yaml
```
## 4. Start agent (to execute flow runs)
Still in the same terminal:
```bash
prefect agent start -q default
```