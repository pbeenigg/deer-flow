"""Quick test for the scheduled tasks router import."""

from app.gateway.routers.scheduled_tasks import router

print("Router imported successfully, prefix:", router.prefix)
print("Routes:")
for route in router.routes:
    if hasattr(route, "methods") and hasattr(route, "path"):
        print(f"  {list(route.methods)} {route.path}")
