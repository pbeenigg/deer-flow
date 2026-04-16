"""Quick test for the full FastAPI app import."""

from app.gateway.app import create_app

app = create_app()
routes = [r.path for r in app.routes if hasattr(r, "path")]
scheduled_routes = [r for r in routes if "scheduled" in r]
print(f"Total routes: {len(routes)}")
print(f"Scheduled task routes: {len(scheduled_routes)}")
for r in sorted(scheduled_routes):
    print(f"  {r}")
print("Full app import successful!")
