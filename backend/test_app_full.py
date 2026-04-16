"""Test that the FastAPI app can be created with the scheduler module."""
from app.gateway.app import create_app

app = create_app()

scheduled_routes = []
for route in app.routes:
    if hasattr(route, 'path') and 'scheduled' in route.path:
        methods = list(route.methods) if hasattr(route, 'methods') else []
        scheduled_routes.append((methods, route.path))

print(f"Scheduled task API routes ({len(scheduled_routes)}):")
for methods, path in sorted(scheduled_routes, key=lambda x: x[1]):
    print(f"  {', '.join(methods):8s} {path}")

print("\nApp created successfully!")
