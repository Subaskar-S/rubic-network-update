# Rubic Wallet - Quick Start Reference

## 🚀 One-Command Startup

### Start Everything (3 Terminals)

**Terminal 1 - Backend Server:**
```bash
target\release\rubic.exe
```
✅ Server: `http://localhost:3000`

**Terminal 2 - Frontend Web App:**
```bash
cd frontend && node node_modules/vite/bin/vite.js
```
✅ Web App: `http://localhost:5173`

**Terminal 3 - Desktop App (Optional):**
```bash
cd src-tauri && cargo tauri dev
```
✅ Desktop App: Opens automatically

## 🔧 Build Commands

### Development Build
```bash
cargo build                    # Debug build
cd frontend && yarn dev        # Development server
```

### Production Build
```bash
cargo build --release                           # Optimized backend
cd frontend && node node_modules/vite/bin/vite.js build  # Production frontend
cd src-tauri && cargo tauri build              # Desktop application
```

## 📡 Quick API Tests

```bash
# Check if backend is running
curl http://localhost:3000/peers

# Check peer limits
curl http://localhost:3000/peers/limit

# Add a peer
curl "http://localhost:3000/peers/add/192.168.1.100:21841"

# Test CORS (from frontend)
curl -H "Origin: http://localhost:5173" http://localhost:3000/peers/limit
```

## 🚨 Quick Fixes

### Port 3000 Busy
```bash
netstat -ano | findstr :3000
taskkill /F /PID <PID>
```

### Frontend Won't Build
```bash
# Fix Git SSH issues
git config --global url."https://github.com/".insteadOf ssh://git@github.com/

# Clean install
cd frontend
rm -rf node_modules yarn.lock
yarn install --network-timeout 100000
```

### Desktop App Won't Start
```bash
# Ensure frontend is built first
cd frontend && node node_modules/vite/bin/vite.js build
cd ../src-tauri && cargo tauri dev
```

## 📊 System Status Check

### Health Check Commands
```bash
# Backend health
curl http://localhost:3000/peers | jq

# Frontend accessibility
curl -I http://localhost:5173

# Build status
cargo check                    # Rust compilation check
cd frontend && yarn build      # Frontend build test
```

### Performance Monitoring
```bash
# Check running processes
tasklist | findstr rubic
tasklist | findstr node

# Memory usage
wmic process where name="rubic.exe" get PageFileUsage
wmic process where name="node.exe" get PageFileUsage
```

## 🔄 Daily Development Workflow

### 1. Start Development Session
```bash
# Terminal 1
target\release\rubic.exe

# Terminal 2  
cd frontend && node node_modules/vite/bin/vite.js

# Open browser to http://localhost:5173
```

### 2. Make Changes
- **Backend**: Edit Rust files, then `cargo build --release`
- **Frontend**: Edit React files, hot reload automatic
- **Desktop**: Changes reflect automatically in dev mode

### 3. Test Changes
```bash
# Quick API test
curl http://localhost:3000/peers

# Frontend test - open browser and verify UI
# Desktop test - check Tauri window
```

### 4. Build for Production
```bash
cargo build --release
cd frontend && node node_modules/vite/bin/vite.js build
cd src-tauri && cargo tauri build
```

## 📁 Key Files & Locations

### Configuration Files
- `src-tauri/tauri.conf.json` - Desktop app config
- `frontend/src/api_config.js` - Backend URL config
- `frontend/package.json` - Frontend dependencies
- `Cargo.toml` - Rust dependencies

### Important Directories
- `target/release/` - Compiled backend binary
- `frontend/dist/` - Production frontend build
- `frontend/src/` - React source code
- `network/src/` - Async network implementation
- `src-tauri/target/` - Desktop app builds

### Log Files & Debugging
- Backend logs: Console output from `rubic.exe`
- Frontend logs: Browser developer console
- Desktop logs: Tauri console output

## 🎯 Success Indicators

### ✅ Everything Working When:
- Backend responds to `curl http://localhost:3000/peers`
- Frontend loads at `http://localhost:5173`
- Desktop app opens without errors
- API calls work from frontend UI
- Real-time updates visible in interface

### ❌ Common Problems:
- Port 3000 already in use → Kill existing process
- Frontend build fails → Check Git/SSH config
- Desktop app won't start → Build frontend first
- API calls fail → Check CORS and backend status

## 🏆 Integration Status

✅ **Async Network**: High-performance peer management  
✅ **React Frontend**: Modern UI with Material-UI  
✅ **Tauri Desktop**: Cross-platform application  
✅ **API Integration**: Full CRUD operations  
✅ **Build System**: Automated and documented  
✅ **Documentation**: Complete guides available  

**Status**: Production Ready 🚀

---

**Quick Reference for Rubic Wallet v2.0**  
**Last Updated**: September 2025  
**Integration**: Complete ✅
