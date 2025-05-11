# Ejabberd Docker Setup

This repository contains a Docker Compose configuration for running Ejabberd XMPP server.

## Prerequisites

- Docker and Docker Compose installed on your system
- Port 5222, 5269, 5280, 5443, 3478, 1883, and 3306 available on your host machine

## Setup Instructions

1. Make sure the directory `C:\ejabberd` and its subdirectory for MySQL data exist on your system. Create them if they don't:
   ```
   mkdir C:\ejabberd
   mkdir C:\ejabberd\mysql
   ```

   Note: While we use backslashes in Windows commands, Docker uses forward slashes in the volume mapping.

2. Add an entry to your hosts file to resolve the hostname hanvi.com to localhost:
   - Open Notepad as Administrator
   - Open the file `C:\Windows\System32\drivers\etc\hosts`
   - Add the following line at the end of the file:
     ```
     127.0.0.1 hanvi.com
     ```
   - Save the file

3. Start the Ejabberd server:
   ```
   docker-compose up -d
   ```

4. To stop the server:
   ```
   docker-compose down
   ```

## Accessing Ejabberd

### Web Admin Interface

- URL: http://localhost:5280/admin
- Username: admin@hanvi.com
- Password: ram

### XMPP Client Connection

You can connect to the Ejabberd server using any XMPP client with these settings:
- Server: hanvi.com (or localhost)
- Port: 5222
- Username: admin
- Domain: hanvi.com
- Password: ram

## Verifying Accessibility

1. **Web Admin Interface**: 
   - Open a browser and navigate to http://localhost:5280/admin
   - Log in with admin@hanvi.com and password ram
   - If you can log in successfully, the web admin interface is accessible

2. **XMPP Client Connection**:
   - Use an XMPP client like Pidgin, Gajim, or Conversations
   - Configure it with the settings mentioned above
   - If you can connect and authenticate, the XMPP service is accessible

## Troubleshooting

- If you encounter permission issues with the volume mapping, ensure that Docker has permission to access the C:\ejabberd directory.
- If ports are not accessible, check if they are being used by other services on your system.
- Check Docker logs for any errors:
  ```
  docker-compose logs ejabberd
  ```
- For MySQL-specific issues:
  ```
  docker-compose logs mysql
  ```
- If ejabberd cannot connect to MySQL:
  1. Ensure MySQL container is running: `docker ps | findstr mysql`
  2. Check MySQL logs for any startup issues
  3. Verify the MySQL credentials in docker-compose.yml match what ejabberd is using
  4. Try connecting to MySQL from the host: `mysql -h 127.0.0.1 -u ejabberd -p`
  5. If needed, you can reset the MySQL data by removing the C:\ejabberd\mysql directory and recreating it

## Configuration

The Ejabberd server is configured with:
- Image: ghcr.io/processone/ejabberd:latest
- Hostname: hanvi.com
- Admin user: admin@hanvi.com with password ram
- Data stored in C:\ejabberd on the host machine
- MySQL database backend for persistent storage

### MySQL Configuration

The setup includes a MySQL database for ejabberd to store user data, messages, and other information:

- MySQL version: 8.0
- Database name: ejabberd
- MySQL root password: ram
- MySQL user: root
- MySQL password: ram
- Data stored in C:\ejabberd\mysql on the host machine
- Port: 3306 (accessible from the host machine)

Ejabberd is configured to use this MySQL instance as its database backend through environment variables in the docker-compose.yml file.
