# ITE501 Starter API (Python/Flask) — Product & Orders

This is a **ready-made REST API** for ITE501 projects. Students should:
1) Download this project, **push to their own GitHub repo**,  
2) Make minor changes (config, small endpoint change, etc.),  
3) Deploy on AWS (EC2 + ALB + ASG) and automate via CI/CD.

## Features
- CRUD for **Products** and **Orders**
- Simple **API-key authentication** via header `x-api-key`
- JSON file persistence (no DB server required)

## Quick Start (Local)
```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
python app.py
```

API will run at: `http://localhost:5000`

## Authentication
Include header:
- `x-api-key: <your API key>`

Default key in `.env.example` is `changeme-ite501`.

## Endpoints
### Health
- `GET /health`

### Products
- `GET /api/products`
- `GET /api/products/<id>`
- `POST /api/products`
- `PUT /api/products/<id>`
- `DELETE /api/products/<id>`

### Orders
- `GET /api/orders`
- `GET /api/orders/<id>`
- `POST /api/orders`
- `PUT /api/orders/<id>`
- `DELETE /api/orders/<id>`

## Sample Requests
```bash
curl -H "x-api-key: changeme-ite501" http://localhost:5000/api/products
```

Create an order:
```bash
curl -X POST http://localhost:5000/api/orders \
  -H "Content-Type: application/json" \
  -H "x-api-key: changeme-ite501" \
  -d '{"customer":"Fatima","items":[{"productId":"p1","qty":2}]}'
```

## Notes for AWS Deployment
- Configure ALB health checks to `/health`.
- Run Flask with **gunicorn** in production (optional). For learning, `python app.py` is acceptable.
- Keep `API_KEY` as an environment variable (do NOT hardcode).

## License
Educational use for ITE501.
