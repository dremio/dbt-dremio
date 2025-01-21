# Initialize the database
echo "Step 1: Starting - Applying DB migrations"
superset db upgrade
echo "Step 1: Complete - Applying DB migrations"

# Create an admin user
echo "Step 2: Starting - Setting up admin user ( admin / admin )"
superset fab create-admin \
              --username admin \
              --firstname Superset \
              --lastname Admin \
              --email admin@superset.com \
              --password "admin"
echo "Step 2: Complete - Setting up admin user"

# Create default roles and permissions
echo "Step 3: Starting - Setting up roles and perms"
superset init
echo "Step 3: Complete - Setting up roles and perms"
