#!/bin/sh

PREFIX="/Library/Apache/ArrowFlightSQLODBC"
USER_ODBCINST_FILE="$HOME/Library/ODBC/odbcinst.ini"
USER_ODBC_FILE="$HOME/Library/ODBC/odbc.ini"
DRIVER_NAME="Apache Arrow Flight SQL ODBC Driver"
DSN_NAME="Apache Arrow Flight SQL ODBC DSN"

touch "$USER_ODBCINST_FILE"
touch "$USER_ODBC_FILE"

if [ $EUID -ne 0 ]; then 
    echo "Please run this script with sudo"
    exit 1
fi

if grep -q "^\[$DRIVER_NAME\]" "$USER_ODBCINST_FILE"; then
  echo "Driver [$DRIVER_NAME] already exists in odbcinst.ini"
else
  echo "Adding [$DRIVER_NAME] to odbcinst.ini..."
  echo "
[$DRIVER_NAME]
Description=An ODBC Driver for Apache Arrow Flight SQL
Driver=/Library/Apache/ArrowFlightSQLODBC/lib/libarrow_flight_sql_odbc.dylib
" >> "$USER_ODBCINST_FILE"
fi

# Check if [ODBC Drivers] section exists
if grep -q '^\[ODBC Drivers\]' "$USER_ODBCINST_FILE"; then
  # Section exists: check if driver entry exists
  if ! grep -q "^${DRIVER_NAME}=" "$USER_ODBCINST_FILE"; then
    # Driver entry does not exist, add under [ODBC Drivers]
    sed -i '' "/^\[ODBC Drivers\]/a\\
${DRIVER_NAME}=Installed
" "$USER_ODBCINST_FILE"
  fi
else
  # Section doesn't exist, append both section and driver entry at end
  {
    echo ""
    echo "[ODBC Drivers]"
    echo "${DRIVER_NAME}=Installed"
  } >> "$USER_ODBCINST_FILE"
fi

if grep -q "^\[$DSN_NAME\]" "$USER_ODBC_FILE"; then
  echo "DSN [$DSN_NAME] already exists in $USER_ODBC_FILE"
else
  echo "Adding [$DSN_NAME] to $USER_ODBC_FILE..."
  cat >> "$USER_ODBC_FILE" <<EOF

[$DSN_NAME]
Description=An ODBC Driver DSN for Apache Arrow Flight SQL
Driver=Apache Arrow Flight SQL ODBC Driver
Host=[HOST]
Port=32010
UID=[USERNAME]
PWD=[PASSWORD]
useEncryption=false
EOF
fi

# Check if [ODBC Data Sources] section exists
if grep -q '^\[ODBC Data Sources\]' "$USER_ODBC_FILE"; then
  # Section exists: check if DSN entry exists
  if ! grep -q "^${DSN_NAME}=" "$USER_ODBC_FILE"; then
    # Add DSN entry under [ODBC Data Sources] section

    # Use awk to insert the line immediately after [ODBC Data Sources]
    awk -v dsn="$DSN_NAME" -v driver="$DRIVER_NAME" '
      $0 ~ /^\[ODBC Data Sources\]/ && !inserted {
        print
        print dsn "=" driver
        inserted=1
        next
      }
      { print }
    ' "$USER_ODBC_FILE" > "${USER_ODBC_FILE}.tmp" && mv "${USER_ODBC_FILE}.tmp" "$USER_ODBC_FILE"
  fi
else
  # Section doesn't exist, append section and DSN entry at end
  {
    echo ""
    echo "[ODBC Data Sources]"
    echo "${DSN_NAME}=${DRIVER_NAME}"
  } >> "$USER_ODBC_FILE"
fi

