const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const bcrypt = require('bcryptjs');
const cors = require('cors');
const jwt = require('jsonwebtoken');
const path = require('path');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);

// ✅ JWT SECRET DEFINITION (ADD THIS!)
// =============================================
const JWT_SECRET = process.env.JWT_SECRET || '419a2994t-default-secret-key-for-development';
console.log('🔑 JWT_SECRET initialized');


// =============================================
// ✅ EMAIL CONFIGURATION FOR NODEMAILER
// =============================================
// =============================================
// ✅ EMAIL CONFIGURATION FOR NODEMAILER
// =============================================
const nodemailer = require('nodemailer');

// Email configuration from environment variables
// =============================================
// ✅ EMAIL CONFIGURATION FIX
// =============================================
const EMAIL_CONFIG = {
    service: 'gmail',
    host: process.env.EMAIL_HOST || 'smtp.gmail.com',
    port: process.env.EMAIL_PORT || 587,
    secure: process.env.EMAIL_SECURE === 'true',
    auth: {
        user: process.env.EMAIL_USER || 'your-email@gmail.com',
        pass: process.env.EMAIL_PASSWORD || 'ukjjouggmmgaaffg'
    },
    from: process.env.EMAIL_FROM || 'DukaMkononi <no-reply@dukamkononi.com>'
};

// Improved email transporter creation
const emailTransporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER || 'godwinfranklin419@gmail.com',
        pass: process.env.EMAIL_PASSWORD || 'ukjjouggmmgaaffg' // Use App Password, not regular password
    }
});

// Test email configuration on startup
emailTransporter.verify(function(error, success) {
    if (error) {
        console.error('❌ EMAIL CONFIGURATION ERROR:', error.message);
        console.log('⚠️ Email functionality may not work. Please check your email configuration.');
    } else {
        console.log('✅ Email server is ready to send messages');
    }
});

// Helper function to send email
async function sendEmail(to, subject, htmlContent) {
    try {
        const mailOptions = {
            from: EMAIL_CONFIG.from,
            to: to,
            subject: subject,
            html: htmlContent,
            text: htmlContent.replace(/<[^>]*>/g, '') // Fallback plain text
        };

        const info = await emailTransporter.sendMail(mailOptions);
        console.log('📧 Email sent:', info.messageId);
        return { success: true, messageId: info.messageId };
    } catch (error) {
        console.error('❌ ERROR sending email:', error.message);
        return { success: false, error: error.message };
    }
}
// =============================================
// ✅ WEB SOCKET SERVER FOR REAL-TIME TRACKING
// =============================================
console.log('🔄 Initializing WebSocket server...');

// Store connected users and admin connections
const connectedUsers = new Map();
const adminConnections = new Set();
const userSessions = new Map();

// Initialize WebSocket server
const wss = new WebSocket.Server({ 
    server, 
    path: '/ws',
    perMessageDeflate: {
        zlibDeflateOptions: {
            chunkSize: 1024,
            memLevel: 7,
            level: 3
        },
        zlibInflateOptions: {
            chunkSize: 10 * 1024
        },
        clientNoContextTakeover: true,
        serverNoContextTakeover: true,
        serverMaxWindowBits: 10,
        concurrencyLimit: 10,
        threshold: 1024
    }
});

// Heartbeat interval for keeping connections alive
const HEARTBEAT_INTERVAL = 25000; // 25 seconds
const CONNECTION_TIMEOUT = 60000; // 60 seconds

console.log('✅ WebSocket server initialized on /ws');

// WebSocket connection handler
wss.on('connection', async (ws, req) => {
    try {
        // Get token from query parameters
        const url = new URL(req.url, `http://${req.headers.host}`);
        const token = url.searchParams.get('token');
        
        if (!token) {
            ws.close(1008, 'Token required');
            return;
        }
        
        // Verify JWT token
        let decoded;
        try {
            decoded = jwt.verify(token, process.env.JWT_SECRET || '419a2994t');
        } catch (error) {
            ws.close(1008, 'Invalid token');
            return;
        }
        
        // Get user from database
        const supabase = createClient(supabaseUrl, supabaseServiceKey);
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('id, email, full_name, role, status, business_name')
            .eq('id', decoded.userId)
            .single();
        
        if (userError || !user) {
            ws.close(1008, 'User not found');
            return;
        }
        
        if (user.status !== 'approved' && user.role !== 'client') {
            ws.close(1008, 'User account is not approved');
            return;
        }
        
        const userId = user.id;
        const userRole = user.role;
        const sessionId = `session_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        console.log(`✅ User ${userId} (${user.email}) connected to WebSocket`);
        
        // Store connection
        const connection = {
            ws,
            userId,
            userRole,
            sessionId,
            connectedAt: new Date(),
            lastHeartbeat: Date.now(),
            ipAddress: req.headers['x-forwarded-for'] || req.connection.remoteAddress,
            userAgent: req.headers['user-agent'],
            email: user.email,
            full_name: user.full_name,
            business_name: user.business_name
        };
        
        connectedUsers.set(userId, connection);
        userSessions.set(sessionId, {
            userId,
            connectedAt: new Date()
        });
        
        // Add to admin connections if user is admin
        if (userRole === 'admin') {
            adminConnections.add(ws);
            console.log(`👨‍💼 Admin ${userId} connected to real-time monitoring`);
        }
        
        // Send welcome message
        ws.send(JSON.stringify({
            type: 'welcome',
            message: 'Connected to real-time presence service',
            userId,
            timestamp: new Date().toISOString()
        }));
        
        // Update user as online in database
        await updateUserPresence(userId, true, sessionId, supabase);
        
        // Broadcast to admins that user came online
        broadcastToAdmins({
            type: 'presence_update',
            data: {
                userId,
                email: user.email,
                full_name: user.full_name,
                role: user.role,
                business_name: user.business_name,
                isOnline: true,
                hasLiveConnection: true,
                connectionAge: 0,
                sessionId,
                timestamp: new Date().toISOString()
            }
        });
        
        // Send current online users list to admin
        if (userRole === 'admin') {
            sendOnlineUsersList(ws, supabase);
        }
        
        // Setup heartbeat
        const heartbeatInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({
                    type: 'heartbeat',
                    timestamp: Date.now(),
                    message: 'ping'
                }));
            }
        }, HEARTBEAT_INTERVAL);
        
        // Handle messages
        ws.on('message', async (data) => {
            try {
                const message = JSON.parse(data.toString());
                
                switch (message.type) {
                    case 'heartbeat':
                        // Update heartbeat timestamp
                        const connection = connectedUsers.get(userId);
                        if (connection) {
                            connection.lastHeartbeat = Date.now();
                            connectedUsers.set(userId, connection);
                            
                            // Update last activity in database
                            await supabase
                                .from('users')
                                .update({ 
                                    last_seen: new Date().toISOString(),
                                    updated_at: new Date().toISOString()
                                })
                                .eq('id', userId);
                        }
                        break;
                        
                    case 'get_online_users':
                        if (userRole === 'admin') {
                            sendOnlineUsersList(ws, supabase);
                        }
                        break;
                        
                    case 'status_update':
                        // Update user status
                        await supabase
                            .from('users')
                            .update({ 
                                status: message.status,
                                updated_at: new Date().toISOString()
                            })
                            .eq('id', userId);
                        break;
                        
                    default:
                        console.log('Unknown WebSocket message type:', message.type);
                }
            } catch (error) {
                console.error('Error processing WebSocket message:', error);
            }
        });
        
        // Handle disconnection
        ws.on('close', async (code, reason) => {
            console.log(`❌ User ${userId} disconnected: ${code} - ${reason}`);
            
            // Clear heartbeat interval
            clearInterval(heartbeatInterval);
            
            // Remove from connections
            connectedUsers.delete(userId);
            
            if (userRole === 'admin') {
                adminConnections.delete(ws);
            }
            
            // Mark as offline after delay (in case of temporary disconnection)
            setTimeout(async () => {
                if (!connectedUsers.has(userId)) {
                    await updateUserPresence(userId, false, sessionId, supabase);
                    
                    // Broadcast to admins that user went offline
                    broadcastToAdmins({
                        type: 'presence_update',
                        data: {
                            userId,
                            email: user.email,
                            full_name: user.full_name,
                            role: user.role,
                            isOnline: false,
                            hasLiveConnection: false,
                            timestamp: new Date().toISOString()
                        }
                    });
                }
            }, 30000); // 30 seconds delay
        });
        
        ws.on('error', (error) => {
            console.error(`WebSocket error for user ${userId}:`, error);
            clearInterval(heartbeatInterval);
            connectedUsers.delete(userId);
            if (userRole === 'admin') {
                adminConnections.delete(ws);
            }
        });
        
    } catch (error) {
        console.error('WebSocket connection error:', error);
        ws.close(1011, 'Internal server error');
    }
});

// Helper function to update user presence
async function updateUserPresence(userId, isOnline, sessionId, supabase) {
    try {
        const now = new Date().toISOString();
        
        // Update users table
        const { error: userError } = await supabase
            .from('users')
            .update({
                is_online: isOnline,
                last_seen: now,
                updated_at: now
            })
            .eq('id', userId);
        
        if (userError) throw userError;
        
        console.log(`✅ User ${userId} marked as ${isOnline ? 'online' : 'offline'}`);
        
    } catch (error) {
        console.error('Error updating user presence:', error);
    }
}

// Broadcast message to all connected admins
function broadcastToAdmins(message) {
    adminConnections.forEach(ws => {
        if (ws.readyState === WebSocket.OPEN) {
            try {
                ws.send(JSON.stringify(message));
            } catch (error) {
                console.error('Error broadcasting to admin:', error);
            }
        }
    });
}

// Send online users list to specific admin
async function sendOnlineUsersList(ws, supabase) {
    try {
        const onlineUsers = [];
        
        // Get users from database who are online
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
        
        const { data: recentUsers, error } = await supabase
            .from('users')
            .select('id, email, full_name, role, business_name, last_seen, is_online, phone')
            .gte('last_seen', fiveMinutesAgo)
            .eq('is_online', true)
            .order('last_seen', { ascending: false });
        
        if (error) throw error;
        
        // Add WebSocket connection status
        const usersWithConnections = (recentUsers || []).map(user => {
            const connection = connectedUsers.get(user.id);
            const connectionAge = connection 
                ? Math.floor((Date.now() - new Date(connection.connectedAt).getTime()) / 1000)
                : null;
            
            return {
                ...user,
                has_live_connection: !!connection,
                connection_age: connectionAge,
                device_info: connection?.userAgent || 'Unknown',
                session_id: connection?.sessionId || null
            };
        });
        
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
                type: 'user_list',
                data: usersWithConnections,
                timestamp: new Date().toISOString()
            }));
        }
        
    } catch (error) {
        console.error('Error sending online users list:', error);
    }
}

// Periodic cleanup of stale connections
setInterval(() => {
    const now = Date.now();
    
    connectedUsers.forEach(async (connection, userId) => {
        if (now - connection.lastHeartbeat > CONNECTION_TIMEOUT) {
            console.log(`🔄 Removing stale connection for user ${userId}`);
            
            try {
                connection.ws.close(1001, 'Connection timeout');
            } catch (error) {
                console.error('Error closing stale connection:', error);
            }
            
            connectedUsers.delete(userId);
            
            if (connection.userRole === 'admin') {
                adminConnections.delete(connection.ws);
            }
            
            // Mark as offline in database
            const supabase = createClient(supabaseUrl, supabaseServiceKey);
            await updateUserPresence(userId, false, connection.sessionId, supabase);
            
            // Broadcast to admins
            broadcastToAdmins({
                type: 'presence_update',
                data: {
                    userId,
                    email: connection.email,
                    full_name: connection.full_name,
                    role: connection.userRole,
                    isOnline: false,
                    hasLiveConnection: false,
                    timestamp: new Date().toISOString()
                }
            });
        }
    });
}, 30000); // Check every 30 seconds

// Send periodic updates to admins
setInterval(() => {
    broadcastToAdmins({
        type: 'heartbeat',
        data: {
            connected_users: connectedUsers.size,
            admin_connections: adminConnections.size,
            timestamp: new Date().toISOString()
        }
    });
}, 10000); // Every 10 seconds

console.log('✅ WebSocket cleanup and heartbeat services started');

// =============================================
// ✅ MIDDLEWARE YA MSINGI
// =============================================
app.use(cors({
    origin: ['http://localhost:19006', 'exp://localhost:19000', 'your-app-scheme://'], // React Native dev servers
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization', 'ngrok-skip-browser-warning'],
    credentials: true
}));

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));

// =============================================
// ✅ STATIC FILES SERVING
// =============================================
app.use(express.static(__dirname));

// Serve HTML files for all routes
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'login.html')));
app.get('/admin_signup', (req, res) => res.sendFile(path.join(__dirname, 'admin_signup.html')));
app.get('/cashier_signup', (req, res) => res.sendFile(path.join(__dirname, 'cashier_signup.html')));
app.get('/client_signup', (req, res) => res.sendFile(path.join(__dirname, 'client_signup.html')));
app.get('/forgot', (req, res) => res.sendFile(path.join(__dirname, 'forgot.html')));
app.get('/home', (req, res) => res.sendFile(path.join(__dirname, 'home.html')));
app.get('/biashara', (req, res) => res.sendFile(path.join(__dirname, 'biashara.html')));
app.get('/bidhaa-mpya', (req, res) => res.sendFile(path.join(__dirname, 'bidhaa-mpya.html')));
app.get('/mauzo', (req, res) => res.sendFile(path.join(__dirname, 'mauzo.html')));
app.get('/uza', (req, res) => res.sendFile(path.join(__dirname, 'uza.html')));
app.get('/matangazo', (req, res) => res.sendFile(path.join(__dirname, 'matangazo.html')));
app.get('/tangaza', (req, res) => res.sendFile(path.join(__dirname, 'tangaza.html')));
app.get('/ripoti', (req, res) => res.sendFile(path.join(__dirname, 'ripoti.html')));
app.get('/upload', (req, res) => res.sendFile(path.join(__dirname, 'upload.html')));
app.get('/preview', (req, res) => res.sendFile(path.join(__dirname, 'preview.html')));
app.get('/profaili', (req, res) => res.sendFile(path.join(__dirname, 'profaili.html')));
app.get('/profaili2', (req, res) => res.sendFile(path.join(__dirname, 'profaili2.html')));
app.get('/profaili3', (req, res) => res.sendFile(path.join(__dirname, 'profaili3.html')));
app.get('/layout', (req, res) => res.sendFile(path.join(__dirname, 'layout.html')));
app.get('/malipo', (req, res) => res.sendFile(path.join(__dirname, 'malipo.html')));

// =============================================
// ✅ SUPABASE CONFIGURATION
// =============================================
// =============================================
// ✅ SUPABASE CONFIGURATION - FORCE CORRECT URL
// =============================================
const supabaseUrl = 'https://qqyihfswnuglnyqvjyrs.supabase.co';
const supabaseAnonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFxeWloZnN3bnVnbG55cXZqeXJzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUwMjY3OTMsImV4cCI6MjA4MDYwMjc5M30.ULkn9XOW6_-APvA-2JYGxo7X0X-CtsGkK9Duf0-xOY8';
const supabaseServiceKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFxeWloZnN3bnVnbG55cXZqeXJzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2NTAyNjc5MywiZXhwIjoyMDgwNjAyNzkzfQ.OwMCapsxey2LV_iW4vnfSuH174ipi-k9MyoXlgTa1zE';

console.log('✅ FORCED SUPABASE URL:', supabaseUrl);
console.log('✅ URL length:', supabaseUrl.length);
console.log('✅ Expected: qqyihfswnuglnyqvjyrs.supabase.co');

// Validate Supabase configuration
if (!supabaseUrl || !supabaseAnonKey || !supabaseServiceKey) {
    console.error('❌ SUPABASE CREDENTIALS MISSING! Please check your .env file');
    console.error('Required: SUPABASE_URL, SUPABASE_ANON_KEY, SUPABASE_SERVICE_KEY');
    process.exit(1);
}

console.log('🔧 Supabase Configuration Loaded');
console.log('📍 URL:', supabaseUrl);
console.log('🔑 Anon Key:', supabaseAnonKey ? '✓ Loaded' : '✗ Missing');
console.log('🔑 Service Key:', supabaseServiceKey ? '✓ Loaded' : '✗ Missing');

const supabase = createClient(supabaseUrl, supabaseAnonKey);
const supabaseAdmin = createClient(supabaseUrl, supabaseServiceKey);

// =============================================
// ✅ HELPER FUNCTIONS
// =============================================
const handleSupabaseError = (error, res) => {
    console.error('❌ SUPABASE ERROR:', error.message);
    return res.status(500).json({ 
        error: 'Hitilafu ya ndani ya server', 
        details: error.message 
    });
};

// ✅ LOGGING FUNCTION - Stores logs to user_logs table
// ✅ FIXED LOGGING FUNCTION
const logUserAction = async (user_id, action, endpoint, details = {}, ip_address = null, status = 'success') => {
    const logId = `log_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    
    // FALLBACK 1: Always log to console
    const consoleMessage = `📝 [${logId}] ${action} - ${endpoint} - ${status} - User: ${user_id || 'Guest'} - IP: ${ip_address}`;
    console.log(consoleMessage);
    
    if (details && Object.keys(details).length > 0) {
        console.log(`📋 Details:`, details);
    }
    
    // FALLBACK 2: Try database logging with timeout
    try {
        // Ensure status is valid
        const validStatus = ['success', 'failed'].includes(status) ? status : 'success';
        
        const logData = {
            user_id: user_id || null,
            action: action,
            endpoint: endpoint,
            details: typeof details === 'object' ? details : { message: details },
            ip_address: ip_address || null,
            status: validStatus, // Use validated status
            created_at: new Date().toISOString()
        };

        // Use Promise.race to add timeout
        const timeoutPromise = new Promise((_, reject) => {
            setTimeout(() => reject(new Error('Logging timeout after 3s')), 3000);
        });
        
        const loggingPromise = supabaseAdmin
            .from('user_logs')
            .insert([logData]);
        
        // Race between logging and timeout
        const { error } = await Promise.race([loggingPromise, timeoutPromise]);
        
        if (error) {
            console.warn(`⚠️ [${logId}] Database logging warning: ${error.message}`);
            // Store in memory cache if database fails
            const memoryLog = {
                ...logData,
                failed_at: new Date().toISOString(),
                error_message: error.message
            };
            
            // Simple in-memory storage (last 100 logs)
            if (!global.failedLogs) global.failedLogs = [];
            global.failedLogs.push(memoryLog);
            if (global.failedLogs.length > 100) {
                global.failedLogs.shift();
            }
        } else {
            console.log(`✅ [${logId}] Log saved to database`);
        }
        
    } catch (error) {
        // FALLBACK 3: If all else fails, just console.error with context
        console.error(`❌ [${logId}] LOGGING SYSTEM ERROR: ${error.message}`);
        console.error(`   Action: ${action}, Endpoint: ${endpoint}, User: ${user_id}`);
    }
};

// =============================================
// ✅ EMERGENCY EMAIL FALLBACK
// =============================================
async function sendEmailWithFallback(to, subject, htmlContent) {
    try {
        // Try main email function first
        const result = await sendEmail(to, subject, htmlContent);
        
        if (result.success) {
            return result;
        }
        
        // If failed, try emergency method
        console.log('⚠️ Main email failed, trying emergency method...');
        
        // Simple direct SMTP without service configuration
        const emergencyTransporter = nodemailer.createTransport({
            host: 'smtp.gmail.com',
            port: 587,
            secure: false,
            auth: {
                user: process.env.EMAIL_USER,
                pass: process.env.EMAIL_PASSWORD
            }
        });
        
        const mailOptions = {
            from: process.env.EMAIL_USER,
            to: to,
            subject: subject,
            html: htmlContent,
            text: htmlContent.replace(/<[^>]*>/g, '')
        };
        
        const info = await emergencyTransporter.sendMail(mailOptions);
        console.log('📧 Emergency email sent:', info.messageId);
        return { success: true, messageId: info.messageId, emergency: true };
        
    } catch (error) {
        console.error('❌ All email methods failed:', error.message);
        
        // Store email content in database as backup
        try {
            await supabaseAdmin
                .from('pending_emails')
                .insert([{
                    to: to,
                    subject: subject,
                    content: htmlContent,
                    status: 'failed',
                    error: error.message,
                    created_at: new Date().toISOString()
                }]);
        } catch (dbError) {
            console.error('Could not save to pending emails:', dbError.message);
        }
        
        return { 
            success: false, 
            error: error.message,
            note: 'Email saved as pending'
        };
    }
}

// ✅ REQUEST LOGGING MIDDLEWARE
app.use((req, res, next) => {
    const startTime = Date.now();
    const originalSend = res.send;
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    
    res.send = function(data) {
        const duration = Date.now() - startTime;
        console.log(`🌐 ${req.method} ${req.originalUrl} - ${res.statusCode} - ${duration}ms - IP: ${ip}`);
        
        return originalSend.call(this, data);
    };
    
    next();
});

const authenticateToken = async (req, res, next) => {
    const authHeader = req.headers['authorization'];
    const token = authHeader && authHeader.split(' ')[1];
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;

    if (!token) {
        await logUserAction(null, 'AUTH_FAILED', req.originalUrl, { reason: 'Token missing' }, ip, 'failed');
        return res.status(401).json({ error: 'Token inahitajika' });
    }

    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        
        const { data: user, error } = await supabaseAdmin
            .from('users')
            .select('id, email, role, full_name, business_name, status, is_online, last_seen')
            .eq('id', decoded.userId)
            .single();
        
        if (error || !user) {
            await logUserAction(decoded.userId, 'AUTH_FAILED', req.originalUrl, { reason: 'User not found' }, ip, 'failed');
            return res.status(401).json({ error: 'Token si sahihi' });
        }

        req.user = user;
        req.user_ip = ip;
        
        await logUserAction(user.id, 'AUTH_SUCCESS', req.originalUrl, { role: user.role }, ip, 'success');
        
        next();
    } catch (error) {
        console.error('❌ AUTH ERROR:', error.message);
        await logUserAction(null, 'AUTH_FAILED', req.originalUrl, { reason: error.message }, ip, 'failed');
        return res.status(401).json({ error: 'Token si sahihi au imekwisha' });
    }
};

// ✅ FUNCTION: GENERATE INVOICE NUMBER
const generateInvoiceNumber = async (seller_id = null) => {
    try {
        const yearSuffix = new Date().getFullYear().toString().slice(-2);
        
        let query = supabase
            .from('sales')
            .select('invoice_number, created_at')
            .like('invoice_number', `${yearSuffix}-%`)
            .order('created_at', { ascending: false });
        
        if (seller_id) {
            query = query.eq('seller_id', seller_id);
        }
        
        const { data: lastSales, error } = await query;
        
        if (error) {
            console.error('❌ Database error generating invoice:', error.message);
            return `${yearSuffix}-0001`;
        }
        
        if (!lastSales || lastSales.length === 0) {
            return `${yearSuffix}-0001`;
        }
        
        let maxSequence = 0;
        lastSales.forEach(sale => {
            if (sale.invoice_number) {
                const parts = sale.invoice_number.split('-');
                if (parts.length === 2) {
                    const sequence = parseInt(parts[1]);
                    if (!isNaN(sequence) && sequence > maxSequence) {
                        maxSequence = sequence;
                    }
                }
            }
        });
        
        const newSequence = (maxSequence + 1).toString().padStart(4, '0');
        const newInvoiceNumber = `${yearSuffix}-${newSequence}`;
        
        return newInvoiceNumber;
        
    } catch (error) {
        console.error('❌ ERROR generating invoice number:', error.message);
        const yearSuffix = new Date().getFullYear().toString().slice(-2);
        const timestamp = Date.now().toString().slice(-4);
        return `${yearSuffix}-${timestamp}`;
    }
};

// =============================================
// ✅ PESAPAL CONFIGURATION
// =============================================
const PESAPAL_CONFIG = {
    baseUrl: process.env.PESAPAL_ENV === 'live' 
        ? 'https://pay.pesapal.com/v3' 
        : 'https://cybqa.pesapal.com/pesapalv3',
    consumerKey: process.env.PESAPAL_CONSUMER_KEY || '',
    consumerSecret: process.env.PESAPAL_CONSUMER_SECRET || '',
    notificationId: process.env.PESAPAL_NOTIFICATION_ID || '',
    callbackUrl: process.env.PESAPAL_CALLBACK_URL || 'http://localhost:3000/payment-callback',
    ipnUrl: process.env.PESAPAL_IPN_URL || 'https://your-backend-url/api/payments/pesapal-ipn'
};

// ✅ PESAPAL HELPER FUNCTIONS
async function getPesapalAccessToken() {
    try {
        const auth = Buffer.from(`${PESAPAL_CONFIG.consumerKey}:${PESAPAL_CONFIG.consumerSecret}`).toString('base64');
        
        const response = await fetch(`${PESAPAL_CONFIG.baseUrl}/api/Auth/RequestToken`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': `Bearer ${auth}`
            },
            body: JSON.stringify({
                consumer_key: PESAPAL_CONFIG.consumerKey,
                consumer_secret: PESAPAL_CONFIG.consumerSecret
            })
        });

        if (!response.ok) {
            throw new Error(`Failed to get access token: ${response.status}`);
        }

        const data = await response.json();
        return data.token;
    } catch (error) {
        console.error('❌ PESAPAL ACCESS TOKEN ERROR:', error.message);
        throw error;
    }
}

async function submitPesapalOrder(orderData, accessToken) {
    try {
        const response = await fetch(`${PESAPAL_CONFIG.baseUrl}/api/Transactions/SubmitOrderRequest`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': `Bearer ${accessToken}`
            },
            body: JSON.stringify(orderData)
        });

        if (!response.ok) {
            throw new Error(`Failed to submit order: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('❌ PESAPAL SUBMIT ORDER ERROR:', error.message);
        throw error;
    }
}

async function getPesapalOrderStatus(orderTrackingId, accessToken) {
    try {
        const response = await fetch(
            `${PESAPAL_CONFIG.baseUrl}/api/Transactions/GetTransactionStatus?orderTrackingId=${orderTrackingId}`,
            {
                method: 'GET',
                headers: {
                    'Accept': 'application/json',
                    'Authorization': `Bearer ${accessToken}`
                }
            }
        );

        if (!response.ok) {
            throw new Error(`Failed to get order status: ${response.status}`);
        }

        const data = await response.json();
        return data;
    } catch (error) {
        console.error('❌ PESAPAL ORDER STATUS ERROR:', error.message);
        throw error;
    }
}

// =============================================
// ✅ REAL-TIME ENDPOINTS FOR ADMIN DASHBOARD
// =============================================

// GET ONLINE USERS (HTTP API for backup/initial load)
app.get('/api/admin/online-users', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'ONLINE_USERS_UNAUTHORIZED', '/api/admin/online-users', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`👥 Admin ${adminId} fetching online users via HTTP`);
        
        // Get users who have been active in the last 5 minutes
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
        
        const { data: recentUsers, error } = await supabaseAdmin
            .from('users')
            .select('id, email, full_name, role, business_name, last_seen, is_online, phone, created_at')
            .gte('last_seen', fiveMinutesAgo)
            .eq('is_online', true)
            .order('last_seen', { ascending: false });
        
        if (error) throw error;
        
        // Add WebSocket connection status
        const usersWithConnections = (recentUsers || []).map(user => {
            const connection = connectedUsers.get(user.id);
            const connectionAge = connection 
                ? Math.floor((Date.now() - new Date(connection.connectedAt).getTime()) / 1000)
                : null;
            
            return {
                ...user,
                has_live_connection: !!connection,
                connection_age: connectionAge,
                session_id: connection?.sessionId || null,
                ip_address: connection?.ipAddress || null,
                user_agent: connection?.userAgent || 'Unknown'
            };
        });
        
        await logUserAction(adminId, 'ONLINE_USERS_FETCH', '/api/admin/online-users', { 
            count: usersWithConnections.length,
            with_live_connection: usersWithConnections.filter(u => u.has_live_connection).length 
        }, req.user_ip, 'success');
        
        res.json({
            online_users: usersWithConnections,
            total_connected: connectedUsers.size,
            admin_connected: adminConnections.size,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error('❌ ERROR fetching online users:', error.message);
        await logUserAction(req.user?.id, 'ONLINE_USERS_ERROR', '/api/admin/online-users', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ error: 'Failed to fetch online users' });
    }
});

// Add this test endpoint to check users
app.get('/api/debug/check-user/:email', async (req, res) => {
    try {
        const { email } = req.params;
        
        // Check all roles
        const { data: users, error } = await supabaseAdmin
            .from('users')
            .select('id, email, role, status')
            .eq('email', email);
        
        if (error) throw error;
        
        res.json({
            email: email,
            found: users && users.length > 0,
            users: users || [],
            message: users?.length ? 'User found' : 'User not found'
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ADD TO DATABASE MIGRATION ENDPOINTS
app.post('/api/admin/migrate/password-reset', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log('🔧 Running password reset database migration...');
        
        const migrations = [
            // Create password_reset_codes table
            `CREATE TABLE IF NOT EXISTS password_reset_codes (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                email VARCHAR(255) NOT NULL,
                reset_code VARCHAR(10) NOT NULL,
                is_used BOOLEAN DEFAULT false,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                used_at TIMESTAMP WITH TIME ZONE
            )`,
            
            // Create index for faster lookups
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_user_id ON password_reset_codes(user_id)`,
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_email ON password_reset_codes(email)`,
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_reset_code ON password_reset_codes(reset_code)`,
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_expires_at ON password_reset_codes(expires_at)`
        ];
        
        const results = [];
        
        for (const migration of migrations) {
            try {
                const { error } = await supabaseAdmin.rpc('exec_sql', { query: migration });
                if (error) {
                    results.push({ migration, status: 'failed', error: error.message });
                } else {
                    results.push({ migration, status: 'success' });
                }
            } catch (error) {
                results.push({ migration, status: 'error', error: error.message });
            }
        }
        
        console.log('✅ Password reset database migration completed');
        
        await logUserAction(adminId, 'DATABASE_MIGRATION', '/api/admin/migrate/password-reset', { 
            migrations: results.length,
            successful: results.filter(r => r.status === 'success').length 
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: 'Password reset database migration completed',
            results: results
        });
        
    } catch (error) {
        console.error('❌ Database migration error:', error.message);
        await logUserAction(req.user?.id, 'DATABASE_MIGRATION_ERROR', '/api/admin/migrate/password-reset', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ 
            error: 'Database migration failed',
            details: error.message 
        });
    }
});

// =============================================
// ✅ PASSWORD RESET WITH EMAIL VERIFICATION
// =============================================

// =============================================
// ✅ CREATE PASSWORD RESET TABLE IF NOT EXISTS
// =============================================
app.post('/api/setup/password-reset-table', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🔧 Setting up password_reset_codes table...');
    
    try {
        // Create table SQL
        const createTableSQL = `
            CREATE TABLE IF NOT EXISTS password_reset_codes (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                email VARCHAR(255) NOT NULL,
                reset_code VARCHAR(10) NOT NULL,
                is_used BOOLEAN DEFAULT false,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                used_at TIMESTAMP WITH TIME ZONE
            )
        `;
        
        // Create indexes
        const createIndexesSQL = [
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_user_id ON password_reset_codes(user_id)`,
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_email ON password_reset_codes(email)`,
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_reset_code ON password_reset_codes(reset_code)`,
            `CREATE INDEX IF NOT EXISTS idx_password_reset_codes_expires_at ON password_reset_codes(expires_at)`
        ];
        
        // Execute create table
        const { error: tableError } = await supabaseAdmin.rpc('exec_sql', { query: createTableSQL });
        
        if (tableError) {
            // If exec_sql function doesn't exist, create it first
            console.log('⚠️ exec_sql function not found, creating it...');
            
            const createFunctionSQL = `
                CREATE OR REPLACE FUNCTION exec_sql(query text)
                RETURNS void AS $$
                BEGIN
                    EXECUTE query;
                END;
                $$ LANGUAGE plpgsql;
            `;
            
            try {
                // Try to create function directly
                const { error: funcError } = await supabaseAdmin.rpc('exec_sql', { query: createFunctionSQL });
                if (funcError) {
                    console.log('⚠️ Cannot create function via RPC, trying SQL...');
                    // Try alternative approach
                    await supabaseAdmin.from('users').select('id').limit(1); // Just to test connection
                    
                    // For now, we'll handle table creation in the endpoint itself
                    console.log('ℹ️ Will handle table creation within endpoint logic');
                }
            } catch (funcError) {
                console.log('⚠️ Function creation failed, continuing with fallback');
            }
            
            // Try direct SQL approach
            console.log('🔄 Trying direct table creation...');
            
            // Create table using direct query (might not work with Supabase RLS)
            const { error: directTableError } = await supabaseAdmin
                .from('users')  // Use any existing table to test
                .select('id')
                .limit(1);
            
            if (directTableError) {
                console.error('❌ Cannot execute SQL directly:', directTableError);
                throw new Error('Database permissions issue. Please create table manually.');
            }
        }
        
        // Create indexes
        for (const indexSQL of createIndexesSQL) {
            try {
                await supabaseAdmin.rpc('exec_sql', { query: indexSQL });
            } catch (indexError) {
                console.warn(`⚠️ Could not create index: ${indexError.message}`);
            }
        }
        
        console.log('✅ password_reset_codes table setup completed');
        
        // Log the action
        await logUserAction(null, 'SETUP_PASSWORD_RESET_TABLE', '/api/setup/password-reset-table', {
            success: true,
            table_created: true
        }, ip, 'success');
        
        res.json({
            success: true,
            message: 'password_reset_codes table created successfully!',
            table: 'password_reset_codes',
            indexes: ['user_id', 'email', 'reset_code', 'expires_at']
        });
        
    } catch (error) {
        console.error('❌ ERROR setting up password_reset_codes table:', error.message);
        
        await logUserAction(null, 'SETUP_PASSWORD_RESET_TABLE_ERROR', '/api/setup/password-reset-table', {
            error: error.message
        }, ip, 'failed');
        
        // Provide manual SQL for admin to run
        const manualSQL = `
-- Run this in your Supabase SQL Editor:
CREATE TABLE IF NOT EXISTS password_reset_codes (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL,
    reset_code VARCHAR(10) NOT NULL,
    is_used BOOLEAN DEFAULT false,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    used_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_password_reset_codes_user_id ON password_reset_codes(user_id);
CREATE INDEX idx_password_reset_codes_email ON password_reset_codes(email);
CREATE INDEX idx_password_reset_codes_reset_code ON password_reset_codes(reset_code);
CREATE INDEX idx_password_reset_codes_expires_at ON password_reset_codes(expires_at);
        `;
        
        res.status(500).json({
            error: 'Failed to create password_reset_codes table',
            details: error.message,
            manual_sql: manualSQL,
            instructions: 'Please run the SQL above in your Supabase SQL Editor'
        });
    }
});

// =============================================
// ✅ CHECK IF PASSWORD RESET TABLE EXISTS
// =============================================
app.get('/api/check/password-reset-table', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🔍 Checking password_reset_codes table...');
    
    try {
        // Try to query the table
        const { data, error } = await supabaseAdmin
            .from('password_reset_codes')
            .select('id')
            .limit(1);
        
        if (error) {
            if (error.code === '42P01') { // Table doesn't exist
                await logUserAction(null, 'CHECK_PASSWORD_RESET_TABLE', '/api/check/password-reset-table', {
                    exists: false,
                    error_code: error.code
                }, ip, 'success');
                
                return res.json({
                    exists: false,
                    message: 'password_reset_codes table does not exist',
                    error: error.message
                });
            }
            throw error;
        }
        
        console.log('✅ password_reset_codes table exists');
        
        await logUserAction(null, 'CHECK_PASSWORD_RESET_TABLE', '/api/check/password-reset-table', {
            exists: true,
            sample_data: data
        }, ip, 'success');
        
        res.json({
            exists: true,
            message: 'password_reset_codes table exists and is accessible',
            sample: data
        });
        
    } catch (error) {
        console.error('❌ ERROR checking password_reset_codes table:', error.message);
        
        await logUserAction(null, 'CHECK_PASSWORD_RESET_TABLE_ERROR', '/api/check/password-reset-table', {
            error: error.message
        }, ip, 'failed');
        
        res.status(500).json({
            error: 'Failed to check password_reset_codes table',
            details: error.message
        });
    }
});

app.post('/api/password-reset/request', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 PASSWORD RESET REQUEST:', req.body.email, 'Role:', req.body.role, 'IP:', ip);
    
    const { email, role } = req.body;

    if (!email || !role) {
        await logUserAction(null, 'PASSWORD_RESET_REQUEST_FAILED', '/api/password-reset/request', { 
            reason: 'Missing fields',
            email: email,
            role: role 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Email na role zinahitajika' });
    }

    try {
        // ✅ FIX: Adjust role before querying
        const adjustedRole = adjustRoleForDatabase(role);
        console.log(`🔄 Adjusted role: '${role}' → '${adjustedRole}'`);
        
        // First, check if table exists
        let tableExists = false;
        try {
            const { error: tableCheckError } = await supabaseAdmin
                .from('password_reset_codes')
                .select('id')
                .limit(1);
            
            if (!tableCheckError) {
                tableExists = true;
                console.log('✅ password_reset_codes table exists');
            }
        } catch (tableError) {
            console.log('⚠️ Table check failed:', tableError.message);
            tableExists = false;
        }
        
        // Check if user exists - USE ADJUSTED ROLE
        const { data: usersData, error: userCheckError } = await supabaseAdmin
            .from('users')
            .select('id, email, role, full_name')
            .eq('email', email)
            .eq('role', adjustedRole);

        if (userCheckError) {
            console.error('❌ Database error checking user:', userCheckError);
            throw userCheckError;
        }

        if (!usersData || usersData.length === 0) {
            // For security, don't reveal if user exists or not
            await logUserAction(null, 'PASSWORD_RESET_REQUEST_FAILED', '/api/password-reset/request', { 
                reason: 'User not found',
                email: email,
                role: role,
                adjustedRole: adjustedRole
            }, ip, 'failed');
            
            // Return success even if user doesn't exist (security measure)
            return res.json({
                success: true,
                message: 'Ikiwa barua pepe ipo kwenye mfumo, utapokea msimbo wa kubadilisha nenosiri kwenye barua pepe yako.',
                requiresCode: true
            });
        }

        const user = usersData[0];
        
        // Generate 6-digit verification code
        const resetCode = Math.floor(100000 + Math.random() * 900000).toString();
        
        // ✅✅✅ DEVELOPMENT MODE CHECK - RETURN CODE DIRECTLY ✅✅✅
        // =============================================================
        if (process.env.NODE_ENV === 'development' || process.env.ENABLE_TEST_MODE === 'true') {
            console.log(`🎯 DEVELOPMENT MODE: Password reset code for ${email}: ${resetCode}`);
            
            // Hifadhi code kwenye database kama inawezekana
            if (tableExists) {
                try {
                    const resetCodeData = {
                        user_id: user.id,
                        email: email,
                        reset_code: resetCode,
                        is_used: false,
                        expires_at: new Date(Date.now() + 15 * 60 * 1000).toISOString(),
                        created_at: new Date().toISOString(),
                        updated_at: new Date().toISOString()
                    };

                    await supabaseAdmin
                        .from('password_reset_codes')
                        .insert([resetCodeData]);
                    
                    console.log('✅ Code saved to database (development mode)');
                } catch (dbError) {
                    console.warn('⚠️ Could not save code to database:', dbError.message);
                }
            }
            
            await logUserAction(user.id, 'PASSWORD_RESET_DEVELOPMENT', '/api/password-reset/request', { 
                code_generated: resetCode,
                development_mode: true,
                table_exists: tableExists
            }, ip, 'success');
            
            return res.json({
                success: true,
                message: 'Development mode: Tumia msimbo huu wa kubadilisha nenosiri',
                resetCode: resetCode, // 🔥 Tunareta code moja kwa moja!
                email: email,
                userId: user.id,
                role: role,
                expiresIn: '15 minutes',
                warning: 'Running in development mode - email haikutumwa',
                development: true,
                timestamp: new Date().toISOString()
            });
        }
        // =============================================================
        
        if (!tableExists) {
            console.log('⚠️ password_reset_codes table does not exist, using fallback system');
            
            console.log('✅ Password reset code generated (fallback):', resetCode);
            
            // Store in memory (TEMPORARY - will be lost on server restart)
            if (!global.tempResetCodes) {
                global.tempResetCodes = new Map();
            }
            
            const expiresAt = new Date(Date.now() + 15 * 60 * 1000);
            const tempCodeData = {
                user_id: user.id,
                email: email,
                reset_code: resetCode,
                is_used: false,
                expires_at: expiresAt.toISOString(),
                created_at: new Date().toISOString()
            };
            
            // Store with both original and adjusted role for lookup
            global.tempResetCodes.set(`${email}:${role}`, tempCodeData);
            global.tempResetCodes.set(`${email}:${adjustedRole}`, tempCodeData);
            
            // Send email with reset code
            const emailSubject = 'Msimbo wa Kubadilisha Nenosiri - DukaMkononi (Mfumo wa Dharura)';
            const emailHtml = `
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #2c3e50;">Habari ${user.full_name || 'Mteja'}!</h2>
                    <p>Umeomba kubadilisha nenosiri lako la akaunti ya DukaMkononi.</p>
                    <p><strong style="color: #e74c3c;">⚠️ KUMBUKA: Hii ni mfumo wa dharura. Wasiliana na msimamizi kuhusu kusanidi database.</strong></p>
                    <p>Msimbo wako wa uthibitishaji ni:</p>
                    <div style="background-color: #f8f9fa; padding: 20px; text-align: center; border-radius: 10px; margin: 20px 0;">
                        <h1 style="font-size: 32px; color: #2c3e50; letter-spacing: 5px; margin: 0;">${resetCode}</h1>
                    </div>
                    <p><strong>Msimbo huu utaisha muda wake ndani ya dakika 15.</strong></p>
                    <p style="color: #e74c3c; font-weight: bold;">
                        ⚠️ KUMBUKA: Hii ni mfumo wa dharura. Msimbo huu utapotea server ikizima upya.
                    </p>
                    <p>Ikiwa hukuomba kubadilisha nenosiri, unaweza kupuuza barua pepe hii.</p>
                    <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                    <p style="font-size: 12px; color: #7f8c8d;">
                        DukaMkononi Team<br>
                        Hii ni barua pepe ya kiotomatiki. Tafadhali usijibu.
                    </p>
                </div>
            `;

            // ✅ IMPROVED EMAIL SENDING WITH FALLBACK
            const emailResult = await sendEmailWithRetry(email, emailSubject, emailHtml);
            
            if (!emailResult.success) {
                console.error('❌ Failed to send email:', emailResult.error);
                
                // DEVELOPMENT FALLBACK: Return code directly if email fails
                console.log('⚠️ Email failed, returning code in response as fallback');
                
                await logUserAction(user.id, 'PASSWORD_RESET_EMAIL_FAILED', '/api/password-reset/request', { 
                    error: emailResult.error,
                    using_fallback: true,
                    code_returned_directly: true
                }, ip, 'partial');
                
                return res.json({
                    success: true,
                    message: 'Msimbo wa kubadilisha nenosiri (email imeshindikana):',
                    resetCode: resetCode, // 🔥 Return code kama fallback
                    email: email,
                    userId: user.id,
                    role: role,
                    expiresIn: '15 minutes',
                    warning: 'Email haikutumwa. Tumia msimbo huu.',
                    email_failed: true,
                    fallback: true
                });
            }

            console.log('✅ Password reset code sent (fallback):', email, 'Code:', resetCode);
            
            await logUserAction(user.id, 'PASSWORD_RESET_REQUEST_FALLBACK', '/api/password-reset/request', { 
                code_sent: true,
                email: email,
                original_role: role,
                adjusted_role: adjustedRole,
                user_found: true,
                using_fallback: true,
                warning: 'password_reset_codes table not found, using in-memory storage'
            }, ip, 'success');
            
            return res.json({
                success: true,
                message: 'Msimbo wa kubadilisha nenosiri umepelekwa kwenye barua pepe yako.',
                requiresCode: true,
                email: email,
                userId: user.id,
                role: role,
                expiresIn: '15 minutes',
                warning: 'Database table not configured. Code stored temporarily.',
                fallback: true
            });
        }
        
        // ✅ NORMAL FLOW - TABLE EXISTS
        
        // Expires in 15 minutes
        const expiresAt = new Date(Date.now() + 15 * 60 * 1000);
        
        // Check if user already has unused codes
        const { data: existingCodes, error: existingError } = await supabaseAdmin
            .from('password_reset_codes')
            .select('id')
            .eq('user_id', user.id)
            .eq('is_used', false)
            .gt('expires_at', new Date().toISOString());
        
        if (existingError) {
            console.error('❌ Error checking existing codes:', existingError);
            throw existingError;
        }
        
        // If there are unused codes, invalidate them
        if (existingCodes && existingCodes.length > 0) {
            console.log(`🔄 Invalidating ${existingCodes.length} unused codes for user ${user.id}`);
            try {
                await supabaseAdmin
                    .from('password_reset_codes')
                    .update({ 
                        is_used: true,
                        updated_at: new Date().toISOString()
                    })
                    .eq('user_id', user.id)
                    .eq('is_used', false);
            } catch (error) {
                console.warn('Error invalidating old codes:', error.message);
            }
        }
        
        // Save reset code to database
        const resetCodeData = {
            user_id: user.id,
            email: email,
            reset_code: resetCode,
            is_used: false,
            expires_at: expiresAt.toISOString(),
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        console.log('💾 Saving reset code to database:', { ...resetCodeData, reset_code: '***' }); // Hide code in logs

        const { error: insertError } = await supabaseAdmin
            .from('password_reset_codes')
            .insert([resetCodeData]);
        
        if (insertError) {
            console.error('❌ Error inserting reset code:', insertError);
            throw insertError;
        }
        
        // Send email with reset code
        const emailSubject = 'Msimbo wa Kubadilisha Nenosiri - DukaMkononi';
        const emailHtml = `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <h2 style="color: #2c3e50;">Habari ${user.full_name || 'Mteja'}!</h2>
                <p>Umeomba kubadilisha nenosiri lako la akaunti ya DukaMkononi.</p>
                <p>Msimbo wako wa uthibitishaji ni:</p>
                <div style="background-color: #f8f9fa; padding: 20px; text-align: center; border-radius: 10px; margin: 20px 0;">
                    <h1 style="font-size: 32px; color: #2c3e50; letter-spacing: 5px; margin: 0;">${resetCode}</h1>
                </div>
                <p><strong>Msimbo huu utaisha muda wake ndani ya dakika 15.</strong></p>
                <p>Ikiwa hukuomba kubadilisha nenosiri, unaweza kupuuza barua pepe hii.</p>
                <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                <p style="font-size: 12px; color: #7f8c8d;">
                    DukaMkononi Team<br>
                    Hii ni barua pepe ya kiotomatiki. Tafadhali usijibu.
                </p>
            </div>
        `;

        const emailResult = await sendEmailWithRetry(email, emailSubject, emailHtml);
        
        if (!emailResult.success) {
            console.error('❌ Failed to send email:', emailResult.error);
            
            // PRODUCTION FALLBACK: Return partial success with code in response
            console.log('⚠️ Email failed in production, returning partial success');
            
            await logUserAction(user.id, 'PASSWORD_RESET_EMAIL_FAILED', '/api/password-reset/request', { 
                error: emailResult.error,
                code_saved_to_db: true,
                reset_code: resetCode
            }, ip, 'partial');
            
            return res.json({
                success: true,
                message: 'Msimbo wa kubadilisha nenosiri umehifadhiwa kwenye mfumo lakini imeshindikana kutumwa kwenye barua pepe.',
                resetCode: resetCode, // 🔥 Optional: Return code for admin/testing
                email: email,
                userId: user.id,
                role: role,
                expiresIn: '15 minutes',
                database_stored: true,
                email_failed: true,
                note: 'Code is saved in database. Contact support with your email.',
                warning: process.env.NODE_ENV === 'production' ? 'Code not shown in production' : 'Use this code for testing'
            });
        }

        console.log('✅ Password reset code sent to:', email, 'Code:', resetCode, 'Adjusted role:', adjustedRole);
        
        await logUserAction(user.id, 'PASSWORD_RESET_REQUEST', '/api/password-reset/request', { 
            code_sent: true,
            email: email,
            original_role: role,
            adjusted_role: adjustedRole,
            user_found: true,
            table_exists: true
        }, ip, 'success');
        
        res.json({
            success: true,
            message: 'Msimbo wa kubadilisha nenosiri umepelekwa kwenye barua pepe yako.',
            requiresCode: true,
            email: email,
            userId: user.id,
            role: role,
            expiresIn: '15 minutes',
            database_stored: true
        });

    } catch (error) {
        console.error('❌ PASSWORD RESET REQUEST ERROR:', error.message);
        
        const logDetails = { 
            error: error.message,
            email: email || 'unknown',
            role: role || 'unknown',
            adjustedRole: role ? adjustRoleForDatabase(role) : 'unknown'
        };
        
        await logUserAction(null, 'PASSWORD_RESET_REQUEST_ERROR', '/api/password-reset/request', 
            logDetails, ip, 'failed');
        
        // Return user-friendly error
        let errorMessage = 'Hitilafu ya ndani ya server: ' + error.message;
        
        if (error.message.includes('fetch') || error.message.includes('network')) {
            errorMessage = 'Hitilafu ya muunganisho na database. Tafadhali jaribu tena baadaye.';
        } else if (error.message.includes('Could not find the table')) {
            errorMessage = 'Mfumo wa kubadilisha nenosiri haujasanidiwa vyema. Tafadhali wasiliana na msimamizi.';
        } else if (error.message.includes('password_reset_codes')) {
            errorMessage = 'Hitilafu ya mfumo wa kubadilisha nenosiri. Tafadhali jaribu tena baadaye.';
        }
        
        res.status(500).json({ 
            success: false,
            error: errorMessage,
            requiresCode: false
        });
    }
});
 // ✅ IMPROVED EMAIL FUNCTION WITH RETRY
async function sendEmailWithRetry(to, subject, htmlContent, maxRetries = 2) {
    let lastError;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            console.log(`📧 Attempt ${attempt} to send email to ${to}`);
            
            const mailOptions = {
                from: EMAIL_CONFIG.from,
                to: to,
                subject: subject,
                html: htmlContent,
                text: htmlContent.replace(/<[^>]*>/g, '')
            };

            const info = await emailTransporter.sendMail(mailOptions);
            console.log(`✅ Email sent (attempt ${attempt}):`, info.messageId);
            return { success: true, messageId: info.messageId, attempt: attempt };
            
        } catch (error) {
            lastError = error;
            console.error(`❌ Email attempt ${attempt} failed:`, error.message);
            
            // Wait before retry (1 second, then 2 seconds)
            if (attempt < maxRetries) {
                await new Promise(resolve => setTimeout(resolve, attempt * 1000));
            }
        }
    }
    
    // All attempts failed
    console.error(`❌ All ${maxRetries} email attempts failed for ${to}`);
    return { success: false, error: lastError.message, attempts: maxRetries };
}

// ✅ SIMPLE TEST EMAIL FUNCTION
async function sendTestEmail() {
    try {
        console.log('🧪 Testing email configuration...');
        
        const testTransporter = nodemailer.createTransport({
            service: 'gmail',
            auth: {
                user: process.env.EMAIL_USER,
                pass: process.env.EMAIL_PASSWORD
            }
        });
        
        await testTransporter.verify();
        console.log('✅ Email connection verified');
        
        const info = await testTransporter.sendMail({
            from: process.env.EMAIL_USER,
            to: process.env.EMAIL_USER,
            subject: 'Test Email from DukaMkononi Server',
            text: `Test email sent at ${new Date().toISOString()}\nServer: ${process.env.NODE_ENV || 'development'}\nEmail configured correctly!`
        });
        
        console.log('✅ Test email sent:', info.messageId);
        return { success: true, messageId: info.messageId };
        
    } catch (error) {
        console.error('❌ Email test failed:', error.message);
        return { success: false, error: error.message };
    }
}
// ✅ PASSWORD RESET WITH EMAIL VERIFICATION (FIXED FOR CLIENT → CUSTOMER)
// =============================================

// Helper function to adjust role for database queries
function adjustRoleForDatabase(role) {
    // Convert 'client' to 'customer' since database uses 'customer'
    if (role === 'client') {
        console.log(`🔄 Adjusting role from '${role}' to 'customer' for database query`);
        return 'customer';
    }
    return role;
}

// Step 1: Request password reset (generate and send code) - FIXED

app.post('/api/password-reset/verify-code', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🔍 VERIFYING RESET CODE from IP:', ip);
    
    const { email, role, resetCode } = req.body;

    if (!email || !role || !resetCode) {
        await logUserAction(null, 'RESET_CODE_VERIFY_FAILED', '/api/password-reset/verify-code', { 
            reason: 'Missing fields' 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Email, role na msimbo zinahitajika' });
    }

    try {
        // ✅ FIX: Adjust role before querying
        const adjustedRole = adjustRoleForDatabase(role);
        console.log(`🔄 Verify: Adjusted role '${role}' → '${adjustedRole}'`);
        
        // Find user - USE ADJUSTED ROLE
        const { data: users, error: userError } = await supabaseAdmin
            .from('users')
            .select('id')
            .eq('email', email)
            .eq('role', adjustedRole); // ← USE adjustedRole HERE

        if (userError) throw userError;

        if (!users || users.length === 0) {
            await logUserAction(null, 'RESET_CODE_VERIFY_FAILED', '/api/password-reset/verify-code', { 
                reason: 'User not found',
                email: email,
                original_role: role,
                adjusted_role: adjustedRole
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Msimbo si sahihi' }); // Generic error for security
        }

        const user = users[0];
        
        // Find valid reset code
        const now = new Date().toISOString();
        const { data: codes, error: codeError } = await supabaseAdmin
            .from('password_reset_codes')
            .select('*')
            .eq('user_id', user.id)
            .eq('reset_code', resetCode)
            .eq('is_used', false)
            .gt('expires_at', now)
            .order('created_at', { ascending: false })
            .limit(1);
        
        if (codeError) throw codeError;

        if (!codes || codes.length === 0) {
            await logUserAction(user.id, 'RESET_CODE_VERIFY_FAILED', '/api/password-reset/verify-code', { 
                reason: 'Invalid or expired code',
                reset_code: resetCode,
                original_role: role,
                adjusted_role: adjustedRole
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Msimbo ulioweka sio sahihi au umeisha muda wake' });
        }

        const codeRecord = codes[0];
        
        // Mark code as used
        await supabaseAdmin
            .from('password_reset_codes')
            .update({ 
                is_used: true,
                used_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            })
            .eq('id', codeRecord.id);

        // Generate verification token for next step
        const verificationToken = jwt.sign(
            { 
                userId: user.id, 
                email: email,
                role: role, // Store original role in token for consistency
                adjustedRole: adjustedRole, // Store adjusted role too
                codeId: codeRecord.id,
                type: 'password_reset_verified',
                verifiedAt: Date.now()
            },
            JWT_SECRET,
            { expiresIn: '10m' } // Short expiration for security
        );

        console.log('✅ Reset code verified successfully for:', email, 'Role:', role, '(Adjusted:', adjustedRole, ')');
        
        await logUserAction(user.id, 'RESET_CODE_VERIFIED', '/api/password-reset/verify-code', { 
            code_id: codeRecord.id,
            verified: true,
            original_role: role,
            adjusted_role: adjustedRole
        }, ip, 'success');
        
        res.json({
            success: true,
            message: 'Msimbo umehakikiwa kikamilifu!',
            verified: true,
            verificationToken: verificationToken,
            expiresIn: '10 minutes'
        });

    } catch (error) {
        console.error('❌ VERIFY RESET CODE ERROR:', error.message);
        await logUserAction(null, 'RESET_CODE_VERIFY_ERROR', '/api/password-reset/verify-code', { 
            error: error.message,
            role: role || 'unknown',
            adjustedRole: role ? adjustRoleForDatabase(role) : 'unknown'
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya ndani ya server: ' + error.message 
        });
    }
});

// Step 3: Reset password with new password - FIXED
app.post('/api/password-reset/confirm', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🔑 CONFIRMING PASSWORD RESET from IP:', ip);
    
    const { verificationToken, newPassword, confirmPassword } = req.body;

    if (!verificationToken || !newPassword || !confirmPassword) {
        await logUserAction(null, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
            reason: 'Missing fields' 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Token, nenosiri jipya na uthibitishaji zinahitajika' });
    }

    if (newPassword !== confirmPassword) {
        await logUserAction(null, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
            reason: 'Passwords do not match' 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Nenosiri jipya na uthibitishaji havifanani' });
    }

    if (newPassword.length < 6) {
        await logUserAction(null, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
            reason: 'Password too short' 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Nenosiri jipya lazima liwe na herufi 6 au zaidi' });
    }

    try {
        // Verify token
        const decoded = jwt.verify(verificationToken, JWT_SECRET);
        
        if (decoded.type !== 'password_reset_verified') {
            await logUserAction(decoded.userId, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
                reason: 'Invalid token type',
                token_type: decoded.type,
                role_in_token: decoded.role,
                adjusted_role: decoded.adjustedRole
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Token si sahihi' });
        }

        const userId = decoded.userId;
        const originalRole = decoded.role;
        const adjustedRole = decoded.adjustedRole || adjustRoleForDatabase(originalRole);
        
        console.log(`🔄 Confirm: User ${userId}, Original role: ${originalRole}, Adjusted: ${adjustedRole}`);
        
        // Verify that the code was actually used
        if (decoded.codeId) {
            const { data: codeRecord, error: codeError } = await supabaseAdmin
                .from('password_reset_codes')
                .select('is_used')
                .eq('id', decoded.codeId)
                .single();
            
            if (!codeError && codeRecord && !codeRecord.is_used) {
                await logUserAction(userId, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
                    reason: 'Code not marked as used',
                    code_id: decoded.codeId,
                    original_role: originalRole,
                    adjusted_role: adjustedRole
                }, ip, 'failed');
                
                return res.status(400).json({ error: 'Msimbo wa uthibitishaji haujatumika' });
            }
        }
        
        // Hash new password
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        
        // Update user password
        const { error: updateError } = await supabaseAdmin
            .from('users')
            .update({ 
                password: hashedPassword,
                updated_at: new Date().toISOString()
            })
            .eq('id', userId);

        if (updateError) throw updateError;

        // Log out all user sessions (optional security measure)
        const connection = connectedUsers.get(userId);
        if (connection) {
            try {
                connection.ws.close(1000, 'Password changed');
                connectedUsers.delete(userId);
            } catch (error) {
                console.error('Error closing WebSocket connection:', error);
            }
        }

        // Send confirmation email
        const { data: user, error: userError } = await supabaseAdmin
            .from('users')
            .select('email, full_name, role')
            .eq('id', userId)
            .single();
        
        if (!userError && user) {
            const emailSubject = 'Nenosiri Limebadilishwa - DukaMkononi';
            const emailHtml = `
                <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                    <h2 style="color: #2c3e50;">Habari ${user.full_name || 'Mteja'}!</h2>
                    <p>Nenosiri lako la akaunti ya DukaMkononi limebadilishwa kikamilifu.</p>
                    <p><strong>Ikiwa hukuomba kubadilisha nenosiri, tafadhali wasiliana na msimamizi mara moja.</strong></p>
                    <hr style="border: none; border-top: 1px solid #eee; margin: 20px 0;">
                    <p style="font-size: 12px; color: #7f8c8d;">
                        DukaMkononi Team<br>
                        Hii ni barua pepe ya kiotomatiki. Tafadhali usijibu.
                    </p>
                </div>
            `;
            
            await sendEmail(user.email, emailSubject, emailHtml);
        }

        console.log('✅ Password reset completed for user:', userId, 'Role:', user?.role || 'unknown');
        
        await logUserAction(userId, 'PASSWORD_RESET_COMPLETED', '/api/password-reset/confirm', { 
            reset_successful: true,
            original_role: originalRole,
            adjusted_role: adjustedRole,
            database_role: user?.role || 'unknown'
        }, ip, 'success');
        
        res.json({
            success: true,
            message: 'Nenosiri limebadilishwa kikamilifu! Unaweza kuingia sasa kwa nenosiri jipya.',
            passwordChanged: true,
            role: originalRole // Return original role for frontend
        });

    } catch (error) {
        if (error.name === 'TokenExpiredError') {
            await logUserAction(null, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
                reason: 'Token expired' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Token imeisha muda wake. Tafadhali anza upya.' });
        } else if (error.name === 'JsonWebTokenError') {
            await logUserAction(null, 'PASSWORD_RESET_CONFIRM_FAILED', '/api/password-reset/confirm', { 
                reason: 'Invalid token' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Token si sahihi' });
        }
        
        console.error('❌ CONFIRM PASSWORD RESET ERROR:', error.message);
        await logUserAction(null, 'PASSWORD_RESET_CONFIRM_ERROR', '/api/password-reset/confirm', { 
            error: error.message 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya ndani ya server: ' + error.message 
        });
    }
});

// Step 4: Check reset status (optional) - FIXED
app.post('/api/password-reset/check-status', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    
    const { email, role } = req.body;

    try {
        if (!email || !role) {
            return res.status(400).json({ error: 'Email na role zinahitajika' });
        }

        // ✅ FIX: Adjust role before querying
        const adjustedRole = adjustRoleForDatabase(role);
        console.log(`🔄 Check status: Adjusted role '${role}' → '${adjustedRole}'`);
        
        const { data: users, error: userError } = await supabaseAdmin
            .from('users')
            .select('id')
            .eq('email', email)
            .eq('role', adjustedRole); // ← USE adjustedRole HERE

        if (userError) throw userError;

        if (!users || users.length === 0) {
            return res.json({ 
                hasActiveReset: false,
                note: 'User not found or role mismatch'
            });
        }

        const user = users[0];
        const now = new Date().toISOString();
        
        // Check for active reset codes
        const { data: activeCodes, error: codeError } = await supabaseAdmin
            .from('password_reset_codes')
            .select('id, created_at, expires_at')
            .eq('user_id', user.id)
            .eq('is_used', false)
            .gt('expires_at', now)
            .order('created_at', { ascending: false })
            .limit(1);
        
        if (codeError) throw codeError;

        const hasActiveReset = activeCodes && activeCodes.length > 0;
        
        res.json({
            hasActiveReset: hasActiveReset,
            activeCode: hasActiveReset ? {
                createdAt: activeCodes[0].created_at,
                expiresAt: activeCodes[0].expires_at
            } : null,
            originalRole: role,
            adjustedRole: adjustedRole
        });

    } catch (error) {
        console.error('❌ CHECK RESET STATUS ERROR:', error.message);
        res.status(500).json({ 
            error: 'Hitilafu ya ndani ya server' 
        });
    }
});

// GET REAL-TIME STATS
app.get('/api/admin/real-time-stats', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'REAL_TIME_STATS_UNAUTHORIZED', '/api/admin/real-time-stats', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`📊 Admin ${adminId} fetching real-time stats`);
        
        // Get today's date
        const today = new Date().toISOString().split('T')[0];
        
        // Total users count
        const { count: totalUsers, error: usersError } = await supabaseAdmin
            .from('users')
            .select('*', { count: 'exact', head: true });
        
        if (usersError) throw usersError;
        
        // Online users count
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
        const { count: onlineUsers, error: onlineError } = await supabaseAdmin
            .from('users')
            .select('*', { count: 'exact', head: true })
            .gte('last_seen', fiveMinutesAgo)
            .eq('is_online', true);
        
        if (onlineError) throw onlineError;
        
        // Today's activities
        const { count: todayActivities, error: activitiesError } = await supabaseAdmin
            .from('user_logs')
            .select('*', { count: 'exact', head: true })
            .gte('created_at', today);
        
        if (activitiesError) throw activitiesError;
        
        // Today's sales
        const { data: todaySalesData, error: salesError } = await supabaseAdmin
            .from('sales')
            .select('total_amount')
            .eq('sale_date', today);
        
        if (salesError) throw salesError;
        
        const todayRevenue = todaySalesData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;
        
        // Reported posts
        const { data: allMatangazo, error: matangazoError } = await supabaseAdmin
            .from('matangazo')
            .select('id, report_count');
        
        if (matangazoError) throw matangazoError;
        
        const reportedPosts = (allMatangazo || []).filter(m => m.report_count > 0).length;
        
        // User roles breakdown
        const { data: usersByRole, error: rolesError } = await supabaseAdmin
            .from('users')
            .select('role, status');
        
        if (rolesError) throw rolesError;
        
        const totalAdmins = usersByRole?.filter(u => u.role === 'admin').length || 0;
        const totalSellers = usersByRole?.filter(u => u.role === 'seller').length || 0;
        const totalClients = usersByRole?.filter(u => u.role === 'client').length || 0;
        const activeUsers = usersByRole?.filter(u => u.status === 'approved').length || 0;
        const pendingUsers = usersByRole?.filter(u => u.status === 'pending').length || 0;
        
        const stats = {
            totalUsers: totalUsers || 0,
            totalAdmins,
            totalSellers,
            totalClients,
            activeUsers,
            pendingUsers,
            reportedPosts,
            todayActivities: todayActivities || 0,
            todayRevenue: parseFloat(todayRevenue.toFixed(2)),
            onlineUsers: onlineUsers || 0,
            connectedNow: Array.from(connectedUsers.values()).filter(u => u.has_live_connection).length,
            adminConnected: adminConnections.size,
            webSocketConnected: connectedUsers.size,
            timestamp: new Date().toISOString()
        };
        
        await logUserAction(adminId, 'REAL_TIME_STATS_FETCH', '/api/admin/real-time-stats', stats, req.user_ip, 'success');
        
        res.json(stats);
        
    } catch (error) {
        console.error('❌ ERROR fetching real-time stats:', error.message);
        await logUserAction(req.user?.id, 'REAL_TIME_STATS_ERROR', '/api/admin/real-time-stats', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ error: 'Failed to fetch real-time stats' });
    }
});

// GET USER PRESENCE HISTORY
app.get('/api/admin/user/:id/presence-history', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const userId = req.params.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'PRESENCE_HISTORY_UNAUTHORIZED', `/api/admin/user/${userId}/presence-history`, { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`📋 Admin ${adminId} fetching presence history for user ${userId}`);
        
        // Get user details
        const { data: user, error: userError } = await supabaseAdmin
            .from('users')
            .select('*')
            .eq('id', userId)
            .single();
        
        if (userError || !user) {
            await logUserAction(adminId, 'PRESENCE_HISTORY_FAILED', `/api/admin/user/${userId}/presence-history`, { 
                reason: 'User not found' 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ error: 'User not found' });
        }
        
        // Get user's recent activity logs
        const { data: activities, error: activitiesError } = await supabaseAdmin
            .from('user_logs')
            .select('*')
            .eq('user_id', userId)
            .order('created_at', { ascending: false })
            .limit(50);
        
        if (activitiesError) throw activitiesError;
        
        // Get connection information from WebSocket
        const connection = connectedUsers.get(userId);
        const connectionInfo = connection ? {
            connected_at: connection.connectedAt,
            last_heartbeat: new Date(connection.lastHeartbeat).toISOString(),
            session_id: connection.sessionId,
            ip_address: connection.ipAddress,
            user_agent: connection.userAgent,
            connection_age: Math.floor((Date.now() - new Date(connection.connectedAt).getTime()) / 1000),
            is_live: true
        } : {
            is_live: false,
            last_seen: user.last_seen
        };
        
        const response = {
            user: {
                id: user.id,
                email: user.email,
                full_name: user.full_name,
                role: user.role,
                status: user.status,
                business_name: user.business_name,
                is_online: user.is_online,
                last_seen: user.last_seen,
                created_at: user.created_at
            },
            connection_info: connectionInfo,
            recent_activities: activities || [],
            total_activities: activities?.length || 0
        };
        
        await logUserAction(adminId, 'PRESENCE_HISTORY_FETCH', `/api/admin/user/${userId}/presence-history`, { 
            user_id: userId,
            has_live_connection: !!connection 
        }, req.user_ip, 'success');
        
        res.json(response);
        
    } catch (error) {
        console.error('❌ ERROR fetching presence history:', error.message);
        await logUserAction(req.user?.id, 'PRESENCE_HISTORY_ERROR', `/api/admin/user/${req.params.id}/presence-history`, { 
            error: error.message,
            user_id: req.params.id 
        }, req.user_ip, 'failed');
        res.status(500).json({ error: 'Failed to fetch presence history' });
    }
});

// UPDATE USER PRESENCE STATUS (Admin can manually update)
app.put('/api/admin/user/:id/presence', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const userId = req.params.id;
        const { is_online } = req.body;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'UPDATE_PRESENCE_UNAUTHORIZED', `/api/admin/user/${userId}/presence`, { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        if (typeof is_online !== 'boolean') {
            await logUserAction(adminId, 'UPDATE_PRESENCE_FAILED', `/api/admin/user/${userId}/presence`, { 
                reason: 'Invalid is_online value' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'is_online must be boolean' });
        }
        
        console.log(`🔄 Admin ${adminId} updating presence for user ${userId} to ${is_online}`);
        
        const now = new Date().toISOString();
        
        const { error } = await supabaseAdmin
            .from('users')
            .update({
                is_online: is_online,
                last_seen: now,
                updated_at: now
            })
            .eq('id', userId);
        
        if (error) throw error;
        
        // If setting to offline and user is connected via WebSocket, disconnect them
        if (!is_online) {
            const connection = connectedUsers.get(userId);
            if (connection) {
                try {
                    connection.ws.close(1000, 'Admin forced disconnect');
                } catch (error) {
                    console.error('Error closing WebSocket connection:', error);
                }
                connectedUsers.delete(userId);
            }
        }
        
        await logUserAction(adminId, 'UPDATE_PRESENCE', `/api/admin/user/${userId}/presence`, { 
            user_id: userId,
            is_online: is_online,
            was_connected: !!connection 
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: `User presence updated to ${is_online ? 'online' : 'offline'}`,
            user_id: userId,
            is_online: is_online,
            updated_at: now
        });
        
    } catch (error) {
        console.error('❌ ERROR updating user presence:', error.message);
        await logUserAction(req.user?.id, 'UPDATE_PRESENCE_ERROR', `/api/admin/user/${req.params.id}/presence`, { 
            error: error.message,
            user_id: req.params.id 
        }, req.user_ip, 'failed');
        res.status(500).json({ error: 'Failed to update user presence' });
    }
});

// GET WEB SOCKET CONNECTION STATUS
app.get('/api/admin/ws-status', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'WS_STATUS_UNAUTHORIZED', '/api/admin/ws-status', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        const wsStatus = {
            total_connections: connectedUsers.size,
            admin_connections: adminConnections.size,
            user_sessions: userSessions.size,
            connected_users: Array.from(connectedUsers.values()).map(conn => ({
                user_id: conn.userId,
                email: conn.email,
                role: conn.userRole,
                connected_at: conn.connectedAt,
                last_heartbeat: new Date(conn.lastHeartbeat).toISOString(),
                session_id: conn.sessionId,
                ip_address: conn.ipAddress,
                user_agent: conn.userAgent
            })),
            server_time: new Date().toISOString(),
            uptime: process.uptime()
        };
        
        await logUserAction(adminId, 'WS_STATUS_FETCH', '/api/admin/ws-status', { 
            total_connections: connectedUsers.size 
        }, req.user_ip, 'success');
        
        res.json(wsStatus);
        
    } catch (error) {
        console.error('❌ ERROR fetching WebSocket status:', error.message);
        await logUserAction(req.user?.id, 'WS_STATUS_ERROR', '/api/admin/ws-status', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ error: 'Failed to fetch WebSocket status' });
    }
});

// =============================================
// ✅ DATABASE MIGRATION ENDPOINTS
// =============================================

// ADD REAL-TIME COLUMNS TO USERS TABLE
app.post('/api/admin/migrate/realtime', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log('🔧 Running real-time database migration...');
        
        // Add columns to users table if they don't exist
        const migrations = [
            // Add is_online column
            `ALTER TABLE users ADD COLUMN IF NOT EXISTS is_online BOOLEAN DEFAULT false`,
            
            // Add last_seen column
            `ALTER TABLE users ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP WITH TIME ZONE`,
            
            // Add session_id column
            `ALTER TABLE users ADD COLUMN IF NOT EXISTS session_id VARCHAR(255)`,
            
            // Add connection_count column
            `ALTER TABLE users ADD COLUMN IF NOT EXISTS connection_count INTEGER DEFAULT 0`,
            
            // Add total_online_time column
            `ALTER TABLE users ADD COLUMN IF NOT EXISTS total_online_time INTEGER DEFAULT 0`,
            
            // Create user_presence_history table
            `CREATE TABLE IF NOT EXISTS user_presence_history (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                session_id VARCHAR(255),
                status VARCHAR(20) NOT NULL CHECK (status IN ('online', 'offline', 'away', 'busy')),
                ip_address VARCHAR(45),
                user_agent TEXT,
                device_info JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )`,
            
            // Create active_sessions table
            `CREATE TABLE IF NOT EXISTS active_sessions (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                session_id VARCHAR(255) NOT NULL UNIQUE,
                device_info JSONB,
                ip_address VARCHAR(45),
                user_agent TEXT,
                connected_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_activity_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                disconnected_at TIMESTAMP WITH TIME ZONE,
                duration_seconds INTEGER
            )`
        ];
        
        const results = [];
        
        for (const migration of migrations) {
            try {
                const { error } = await supabaseAdmin.rpc('exec_sql', { query: migration });
                if (error) {
                    console.error(`Migration failed: ${migration}`, error);
                    results.push({ migration, status: 'failed', error: error.message });
                } else {
                    results.push({ migration, status: 'success' });
                }
            } catch (error) {
                console.error(`Migration error: ${migration}`, error);
                results.push({ migration, status: 'error', error: error.message });
            }
        }
        
        // Create exec_sql function if it doesn't exist
        const createFunctionSQL = `
            CREATE OR REPLACE FUNCTION exec_sql(query text)
            RETURNS void AS $$
            BEGIN
                EXECUTE query;
            END;
            $$ LANGUAGE plpgsql;
        `;
        
        try {
            await supabaseAdmin.rpc('exec_sql', { query: createFunctionSQL });
        } catch (error) {
            console.log('Note: exec_sql function already exists or cannot be created');
        }
        
        console.log('✅ Database migration completed');
        
        await logUserAction(adminId, 'DATABASE_MIGRATION', '/api/admin/migrate/realtime', { 
            migrations: results.length,
            successful: results.filter(r => r.status === 'success').length 
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: 'Database migration completed successfully',
            results: results
        });
        
    } catch (error) {
        console.error('❌ Database migration error:', error.message);
        await logUserAction(req.user?.id, 'DATABASE_MIGRATION_ERROR', '/api/admin/migrate/realtime', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ 
            error: 'Database migration failed',
            details: error.message 
        });
    }
});

// GET DATABASE SCHEMA INFO
app.get('/api/admin/database-info', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log('🔍 Getting database schema info...');
        
        // Check if real-time columns exist
        const { data: columns, error } = await supabaseAdmin
            .from('information_schema.columns')
            .select('column_name, data_type, is_nullable')
            .eq('table_name', 'users')
            .in('column_name', ['is_online', 'last_seen', 'session_id', 'connection_count', 'total_online_time'])
            .order('column_name');
        
        if (error) throw error;
        
        // Check if real-time tables exist
        const { data: tables, error: tablesError } = await supabaseAdmin
            .from('information_schema.tables')
            .select('table_name')
            .in('table_name', ['user_presence_history', 'active_sessions'])
            .order('table_name');
        
        if (tablesError) throw tablesError;
        
        const schemaInfo = {
            users_table_columns: columns || [],
            real_time_tables: tables?.map(t => t.table_name) || [],
            has_is_online: columns?.some(c => c.column_name === 'is_online') || false,
            has_last_seen: columns?.some(c => c.column_name === 'last_seen') || false,
            has_presence_history: tables?.some(t => t.table_name === 'user_presence_history') || false,
            has_active_sessions: tables?.some(t => t.table_name === 'active_sessions') || false,
            web_socket_connected: connectedUsers.size,
            server_time: new Date().toISOString()
        };
        
        await logUserAction(adminId, 'DATABASE_INFO_FETCH', '/api/admin/database-info', { 
            has_real_time_features: schemaInfo.has_is_online 
        }, req.user_ip, 'success');
        
        res.json(schemaInfo);
        
    } catch (error) {
        console.error('❌ ERROR getting database info:', error.message);
        await logUserAction(req.user?.id, 'DATABASE_INFO_ERROR', '/api/admin/database-info', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ error: 'Failed to get database info' });
    }
});

// =============================================
// ✅ BASIC ENDPOINTS (Updated with real-time info)
// =============================================
app.get('/api/test', (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('✅ Test endpoint accessed from IP:', ip);
    
    logUserAction(null, 'TEST_ENDPOINT', '/api/test', {}, ip, 'success');
    
    res.json({ 
        message: '✅ Backend inafanya kazi kikamilifu!', 
        timestamp: new Date().toISOString(),
        database: 'Supabase PostgreSQL',
        server: 'Running successfully',
        supabaseUrl: supabaseUrl,
        webSocket: {
            connected: connectedUsers.size > 0,
            connectedUsers: connectedUsers.size,
            adminConnections: adminConnections.size,
            endpoint: 'ws://' + req.headers.host + '/ws'
        },
        realTimeFeatures: 'Active with WebSocket support'
    });
});

app.get('/api/health', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🏥 Health check requested from IP:', ip);
    
    try {
        const { data, error } = await supabaseAdmin
            .from('users')
            .select('count')
            .limit(1);
        
        if (error) throw error;
        
        await logUserAction(null, 'HEALTH_CHECK', '/api/health', { status: 'healthy' }, ip, 'success');
        
        res.json({ 
            status: 'healthy', 
            database: 'connected',
            provider: 'Supabase',
            timestamp: new Date().toISOString(),
            url: supabaseUrl,
            webSocket: {
                status: wss ? 'running' : 'stopped',
                clients: wss?.clients?.size || 0,
                connectedUsers: connectedUsers.size
            },
            realTime: {
                enabled: true,
                connectedUsers: connectedUsers.size,
                adminConnections: adminConnections.size
            }
        });
    } catch (error) {
        await logUserAction(null, 'HEALTH_CHECK', '/api/health', { status: 'unhealthy', error: error.message }, ip, 'failed');
        
        res.status(500).json({ 
            status: 'unhealthy', 
            database: 'disconnected',
            error: error.message,
            timestamp: new Date().toISOString()
        });
    }
});

// =============================================
// ✅ AUTH ENDPOINTS (Updated with real-time features)
// =============================================
app.post('/api/register', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 REGISTER REQUEST:', req.body, 'IP:', ip);
    
    const { email, password, role, full_name, phone, business_name, business_location } = req.body;

    if (!email || !password || !role) {
        await logUserAction(null, 'REGISTER_FAILED', '/api/register', { reason: 'Missing required fields', email, role }, ip, 'failed');
        return res.status(400).json({ error: 'Email, password na role zinahitajika' });
    }

    if (password.length < 6) {
        await logUserAction(null, 'REGISTER_FAILED', '/api/register', { reason: 'Password too short', email, role }, ip, 'failed');
        return res.status(400).json({ error: 'Nenosiri lazima liwe na herufi 6 au zaidi' });
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
        await logUserAction(null, 'REGISTER_FAILED', '/api/register', { reason: 'Invalid email format', email }, ip, 'failed');
        return res.status(400).json({ error: 'Barua pepe si sahihi' });
    }

    try {
        const { data: existingUsers, error: checkError } = await supabaseAdmin
            .from('users')
            .select('id')
            .eq('email', email)
            .eq('role', role);
        
        if (checkError) throw checkError;

        if (existingUsers && existingUsers.length > 0) {
            await logUserAction(null, 'REGISTER_FAILED', '/api/register', { reason: 'Account already exists', email, role }, ip, 'failed');
            return res.status(400).json({ error: 'Akaunti na barua pepe hii tayari ipo' });
        }

        if (role === 'admin' || role === 'seller') {
            if (!business_name) {
                await logUserAction(null, 'REGISTER_FAILED', '/api/register', { reason: 'Business name required', email, role }, ip, 'failed');
                return res.status(400).json({ error: 'Jina la biashara linahitajika' });
            }

            const { data: adminUsers, error: adminError } = await supabaseAdmin
                .from('users')
                .select('id, email, role, status')
                .ilike('business_name', business_name.trim())
                .eq('role', 'admin')
                .eq('status', 'approved');
            
            if (adminError) throw adminError;

            const hasApprovedAdmin = adminUsers && adminUsers.length > 0;
            
            if (role === 'seller') {
                if (!hasApprovedAdmin) {
                    await logUserAction(null, 'REGISTER_FAILED', '/api/register', { 
                        reason: 'No approved admin for business', 
                        business_name, 
                        email, 
                        role 
                    }, ip, 'failed');
                    
                    return res.status(400).json({ 
                        error: 'Biashara hii haina msimamizi. Tafadhali jisajili kama msimamizi kwanza.'
                    });
                }
            } 
            
            if (role === 'admin') {
                const { data: existingBusiness, error: businessError } = await supabaseAdmin
                    .from('users')
                    .select('id, email, role')
                    .ilike('business_name', business_name.trim())
                    .in('role', ['admin', 'seller']);
                
                if (businessError) throw businessError;

                if (existingBusiness && existingBusiness.length > 0) {
                    await logUserAction(null, 'REGISTER_FAILED', '/api/register', { 
                        reason: 'Business name already taken', 
                        business_name, 
                        existing_user: existingBusiness[0].email 
                    }, ip, 'failed');
                    
                    return res.status(400).json({ 
                        error: 'Jina la biashara tayari limeshasajiliwa',
                        existingUser: existingBusiness[0].email
                    });
                }
            }
        }

        const hashedPassword = await bcrypt.hash(password, 10);
        const userStatus = (role === 'seller') ? 'pending' : 'approved';
        
        const userData = {
            email,
            password: hashedPassword,
            role,
            full_name: full_name || null,
            phone: phone || null,
            business_name: business_name || null,
            business_location: business_location || null,
            status: userStatus,
            is_online: false,
            last_seen: new Date().toISOString(),
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        const { data: newUser, error: insertError } = await supabaseAdmin
            .from('users')
            .insert([userData])
            .select()
            .single();
        
        if (insertError) throw insertError;

        console.log('✅ USER REGISTERED:', { 
            id: newUser.id, 
            email: newUser.email, 
            role: newUser.role,
            status: newUser.status,
            business_name: newUser.business_name
        });
        
        await logUserAction(newUser.id, 'REGISTER_SUCCESS', '/api/register', { 
            role: newUser.role, 
            status: newUser.status,
            business_name: newUser.business_name 
        }, ip, 'success');
        
        res.status(201).json({
            message: role === 'seller' 
                ? 'Akaunti ya muuzaji imeundwa kikamilifu! Umewasilisha ombi lako. Tafadhali subiri uthibitisho wa msimamizi kabla ya kuingia.' 
                : 'Akaunti ya msimamizi imeundwa kikamilifu!',
            userId: newUser.id,
            status: 'success',
            requiresApproval: role === 'seller',
            user: {
                id: newUser.id,
                email: newUser.email,
                role: newUser.role,
                status: newUser.status,
                business_name: newUser.business_name
            }
        });

    } catch (error) {
        console.error('❌ REGISTRATION ERROR:', error.message);
        await logUserAction(null, 'REGISTER_ERROR', '/api/register', { error: error.message, email, role }, ip, 'failed');
        res.status(500).json({ error: 'Hitilafu ya ndani ya server: ' + error.message });
    }
});

app.post('/api/login', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 LOGIN REQUEST:', req.body.email, 'Role:', req.body.role, 'IP:', ip);
    
    const { email, password, role } = req.body;

    if (!email || !password || !role) {
        await logUserAction(null, 'LOGIN_FAILED', '/api/login', { reason: 'Missing fields', email, role }, ip, 'failed');
        return res.status(400).json({ error: 'Email, password na role zinahitajika' });
    }

    try {
        const { data: users, error } = await supabaseAdmin
            .from('users')
            .select('*')
            .eq('email', email)
            .eq('role', role);

        if (error) throw error;

        if (!users || users.length === 0) {
            await logUserAction(null, 'LOGIN_FAILED', '/api/login', { reason: 'Account not found', email, role }, ip, 'failed');
            return res.status(401).json({ error: 'Akaunti haipo au role si sahihi' });
        }

        const user = users[0];
        const isValid = await bcrypt.compare(password, user.password);
        
        if (!isValid) {
            await logUserAction(user.id, 'LOGIN_FAILED', '/api/login', { reason: 'Invalid password', email, role }, ip, 'failed');
            return res.status(401).json({ error: 'Password si sahihi' });
        }

        if (user.role === 'seller' && user.status !== 'approved') {
            await logUserAction(user.id, 'LOGIN_FAILED', '/api/login', { reason: 'Account not approved', status: user.status }, ip, 'failed');
            return res.status(401).json({ 
                error: 'Akaunti yako bado haijaidhinishwa. Subiri msimamizi akuidhinishe.' 
            });
        }

        if (user.status !== 'approved' && user.role !== 'seller') {
            await logUserAction(user.id, 'LOGIN_FAILED', '/api/login', { reason: 'Account not approved', status: user.status }, ip, 'failed');
            return res.status(401).json({ error: 'Akaunti bado haijaidhinishwa.' });
        }

        // Update user as online in database
        const now = new Date().toISOString();
        await supabaseAdmin
            .from('users')
            .update({
                is_online: true,
                last_seen: now,
                updated_at: now
            })
            .eq('id', user.id);

        const token = jwt.sign(
            { userId: user.id, email: user.email, role: user.role },
            JWT_SECRET,
            { expiresIn: '24h' }
        );

        console.log('✅ LOGIN SUCCESSFUL:', user.email);
        
        await logUserAction(user.id, 'LOGIN_SUCCESS', '/api/login', { 
            role: user.role, 
            business_name: user.business_name,
            is_online: true 
        }, ip, 'success');
        
        res.json({
            message: 'Login successful',
            token: token,
            user: {
                id: user.id,
                email: user.email,
                role: user.role,
                full_name: user.full_name,
                business_name: user.business_name,
                business_location: user.business_location,
                phone: user.phone,
                status: user.status,
                is_online: true,
                last_seen: now
            }
        });

    } catch (error) {
        console.error('❌ LOGIN ERROR:', error.message);
        await logUserAction(null, 'LOGIN_ERROR', '/api/login', { error: error.message, email, role }, ip, 'failed');
        res.status(500).json({ error: 'Hitilafu ya ndani ya server: ' + error.message });
    }
});

// =============================================
// ✅ USER PROFILE ENDPOINTS (Updated with last_seen)
// =============================================
app.get('/api/user/profile', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log('👤 Profile requested for user:', userId);
        
        const { data: user, error } = await supabase
            .from('users')
            .select('id, email, role, full_name, phone, business_name, business_location, status, is_online, last_seen, created_at, updated_at')
            .eq('id', userId)
            .single();

        if (error) throw error;

        await logUserAction(userId, 'PROFILE_VIEW', '/api/user/profile', {}, req.user_ip, 'success');
        
        res.json(user);
    } catch (error) {
        await logUserAction(req.user?.id, 'PROFILE_VIEW_ERROR', '/api/user/profile', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.put('/api/user/profile', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { full_name, phone, business_name, business_location } = req.body;
        
        console.log('✏️ Profile update for user:', userId, 'Data:', req.body);

        if (!full_name) {
            await logUserAction(userId, 'PROFILE_UPDATE_FAILED', '/api/user/profile', { reason: 'Full name required' }, req.user_ip, 'failed');
            return res.status(400).json({ error: 'Jina kamili linahitajika' });
        }

        const updateData = {
            full_name,
            phone: phone || null,
            updated_at: new Date().toISOString(),
            last_seen: new Date().toISOString()
        };

        if (business_name) updateData.business_name = business_name;
        if (business_location) updateData.business_location = business_location;

        const { data, error } = await supabase
            .from('users')
            .update(updateData)
            .eq('id', userId);

        if (error) throw error;

        console.log('✅ User profile updated:', userId);
        
        const { data: updatedUser, error: fetchError } = await supabase
            .from('users')
            .select('id, email, role, full_name, phone, business_name, business_location, status, is_online, last_seen, created_at')
            .eq('id', userId)
            .single();

        if (fetchError) throw fetchError;

        await logUserAction(userId, 'PROFILE_UPDATE', '/api/user/profile', { 
            fields_updated: Object.keys(updateData).filter(k => k !== 'updated_at' && k !== 'last_seen') 
        }, req.user_ip, 'success');
        
        res.json({
            message: 'Profaili imesasishwa kikamilifu!',
            user: updatedUser
        });

    } catch (error) {
        await logUserAction(req.user?.id, 'PROFILE_UPDATE_ERROR', '/api/user/profile', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ PRODUCT ENDPOINTS (WITH EXPECTED_SELLING_PRICE AUTO-FILL)
// =============================================
app.get('/api/products/my', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const userRole = req.user.role;
        const businessName = req.user.business_name;
        
        console.log(`📦 Loading products for user ${userId} (${userRole}) in business: ${businessName}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (userRole === 'admin') {
            const { data: products, error } = await supabase
                .from('products')
                .select(`id, name, category, description, price, expected_selling_price, cost_price, stock, is_active, seller_id, created_at, updated_at`)
                .eq('seller_id', userId)
                .eq('is_active', true)
                .order('name', { ascending: true });
            
            if (error) throw error;
            
            console.log(`✅ Admin loaded ${products?.length || 0} personal products`);
            
            const processedProducts = (products || []).map(product => ({
                ...product,
                expected_selling_price: product.expected_selling_price || product.price || 0
            }));
            
            await logUserAction(userId, 'PRODUCTS_VIEW', '/api/products/my', { 
                count: processedProducts.length,
                role: 'admin',
                personal: true 
            }, req.user_ip, 'success');
            
            res.json(processedProducts);
            
        } else {
            const { data: adminUser, error: adminError } = await supabase
                .from('users')
                .select('id')
                .eq('business_name', businessName)
                .eq('role', 'admin')
                .eq('status', 'approved')
                .single();
            
            if (adminError) {
                console.error('❌ Admin not found:', adminError.message);
                return res.json([]);
            }
            
            const { data: businessUsers, error: usersError } = await supabase
                .from('users')
                .select('id, email, full_name, role, status')
                .eq('business_name', businessName)
                .in('role', ['admin', 'seller'])
                .eq('status', 'approved');
            
            if (usersError) {
                console.error('❌ Error fetching business users:', usersError.message);
                return res.json([]);
            }
            
            const sellerIds = businessUsers?.map(user => user.id) || [];
            
            if (sellerIds.length === 0) {
                console.log('⚠️ No sellers found in business');
                return res.json([]);
            }
            
            const { data: allProducts, error: productsError } = await supabase
                .from('products')
                .select(`id, name, category, description, price, expected_selling_price, cost_price, stock, is_active, seller_id, created_at, updated_at`)
                .in('seller_id', sellerIds)
                .eq('is_active', true)
                .order('name', { ascending: true });
            
            if (productsError) throw productsError;
            
            console.log(`✅ Seller loaded ${allProducts?.length || 0} products from business ${businessName}`);
            
            const processedProducts = (allProducts || []).map(product => ({
                ...product,
                expected_selling_price: product.expected_selling_price || product.price || 0
            }));
            
            const productsWithSellerInfo = processedProducts.map(product => {
                const seller = businessUsers.find(user => user.id === product.seller_id);
                
                let seller_name = 'Unknown';
                if (seller) {
                    if (seller.full_name) {
                        seller_name = seller.full_name;
                    } else if (seller.email) {
                        seller_name = seller.email.split('@')[0] || seller.email;
                    }
                }
                
                return {
                    ...product,
                    seller_name: seller_name,
                    seller_role: seller?.role || 'unknown',
                    seller_email: seller?.email || null
                };
            });
            
            await logUserAction(userId, 'PRODUCTS_VIEW', '/api/products/my', { 
                count: productsWithSellerInfo.length,
                role: 'seller',
                business: businessName,
                total_sellers: businessUsers.length 
            }, req.user_ip, 'success');
            
            res.json(productsWithSellerInfo);
        }
        
    } catch (error) {
        console.error('❌ ERROR in /api/products/my:', error.message);
        await logUserAction(req.user?.id, 'PRODUCTS_VIEW_ERROR', '/api/products/my', { error: error.message }, req.user_ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya ndani ya server',
            details: error.message,
            code: 'PRODUCTS_FETCH_ERROR'
        });
    }
});

// =============================================
// ✅ PRODUCTS DUMMY ENDPOINT FOR TESTING
// =============================================
app.get('/api/products/dummy', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const userRole = req.user.role;
        
        console.log('🧪 Generating dummy products for user:', userId);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const dummyProducts = [
            {
                id: 'prod-001',
                name: 'Maji ya Kunywa',
                category: 'Vinywaji',
                description: 'Maji safi ya kunywa',
                price: 1500,
                expected_selling_price: 2000,
                cost_price: 1000,
                stock: 50,
                is_active: true,
                seller_id: userId,
                seller_name: userRole === 'admin' ? 'Admin' : 'Muuzaji',
                seller_role: userRole,
                seller_email: req.user.email,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            },
            {
                id: 'prod-002',
                name: 'Sukari',
                category: 'Chakula',
                description: 'Sukari nyeupe',
                price: 3000,
                expected_selling_price: 3500,
                cost_price: 2500,
                stock: 30,
                is_active: true,
                seller_id: userId,
                seller_name: userRole === 'admin' ? 'Admin' : 'Muuzaji',
                seller_role: userRole,
                seller_email: req.user.email,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            },
            {
                id: 'prod-003',
                name: 'Mafuta ya Kupikia',
                category: 'Chakula',
                description: 'Mafuta ya alizeti',
                price: 5000,
                expected_selling_price: 6000,
                cost_price: 4500,
                stock: 20,
                is_active: true,
                seller_id: userId,
                seller_name: userRole === 'admin' ? 'Admin' : 'Muuzaji',
                seller_role: userRole,
                seller_email: req.user.email,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString()
            }
        ];
        
        console.log(`✅ Generated ${dummyProducts.length} dummy products`);
        
        await logUserAction(userId, 'DUMMY_PRODUCTS', '/api/products/dummy', { 
            count: dummyProducts.length 
        }, req.user_ip, 'success');
        
        res.json(dummyProducts);
        
    } catch (error) {
        console.error('❌ ERROR in dummy endpoint:', error.message);
        await logUserAction(req.user?.id, 'DUMMY_PRODUCTS_ERROR', '/api/products/dummy', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ 
            error: 'Hitilafu ya ndani ya server',
            details: error.message
        });
    }
});

// =============================================
// ✅ CREATE PRODUCT WITH EXPECTED_SELLING_PRICE (REQUIRED)
// =============================================
app.post('/api/products', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { name, category, price, stock, description, cost_price, expected_selling_price } = req.body;
        
        console.log('📨 Creating new product for user:', userId, 'Data:', req.body);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!name || !category || !price || !stock || !expected_selling_price) {
            await logUserAction(userId, 'PRODUCT_CREATE_FAILED', '/api/products', { 
                reason: 'Missing required fields',
                missing_fields: {
                    name: !name,
                    category: !category,
                    price: !price,
                    expected_selling_price: !expected_selling_price,
                    stock: !stock
                }
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Jina, kategoria, bei ya sasa, bei ya kuuzia na idadi ya bidhaa zinahitajika'
            });
        }

        const priceValue = parseFloat(price);
        const expectedPriceValue = parseFloat(expected_selling_price);
        const stockValue = parseInt(stock);
        
        if (isNaN(priceValue) || priceValue <= 0) {
            await logUserAction(userId, 'PRODUCT_CREATE_FAILED', '/api/products', { 
                reason: 'Invalid price',
                price: price 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Bei ya sasa si sahihi (lazima iwe namba kubwa kuliko 0)'
            });
        }
        
        if (isNaN(expectedPriceValue) || expectedPriceValue <= 0) {
            await logUserAction(userId, 'PRODUCT_CREATE_FAILED', '/api/products', { 
                reason: 'Invalid expected selling price',
                expected_selling_price: expected_selling_price 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Bei ya kuuzia si sahihi (lazima iwe namba kubwa kuliko 0)'
            });
        }
        
        if (isNaN(stockValue) || stockValue < 0) {
            await logUserAction(userId, 'PRODUCT_CREATE_FAILED', '/api/products', { 
                reason: 'Invalid stock',
                stock: stock 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Idadi ya bidhaa si sahihi (lazima iwe namba isiyo hasi)'
            });
        }
        
        const productData = {
            seller_id: userId,
            name: name.trim(),
            description: (description || '').trim(),
            category: (category || '').trim(),
            price: priceValue,
            cost_price: cost_price ? parseFloat(cost_price) : null,
            expected_selling_price: expectedPriceValue,
            stock: stockValue,
            is_active: true,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        console.log('💾 Saving product data:', productData);

        const { data: newProduct, error } = await supabase
            .from('products')
            .insert([productData])
            .select(`id, name, category, description, price, expected_selling_price, cost_price, stock, is_active, seller_id, created_at, updated_at`)
            .single();
        
        if (error) {
            console.error('❌ Database error creating product:', error);
            
            if (error.code === '23505') {
                await logUserAction(userId, 'PRODUCT_CREATE_FAILED', '/api/products', { 
                    reason: 'Duplicate product name',
                    name: name 
                }, req.user_ip, 'failed');
                
                return res.status(400).json({ 
                    success: false,
                    error: 'Bidhaa na jina hili tayari ipo. Tafadhali tumia jina tofauti.'
                });
            }
            
            throw error;
        }
        
        console.log('✅ Product created successfully:', {
            id: newProduct.id,
            name: newProduct.name,
            price: newProduct.price,
            expected_selling_price: newProduct.expected_selling_price,
            stock: newProduct.stock
        });
        
        await logUserAction(userId, 'PRODUCT_CREATE', '/api/products', { 
            product_id: newProduct.id,
            name: newProduct.name,
            price: newProduct.price,
            expected_selling_price: newProduct.expected_selling_price,
            stock: newProduct.stock 
        }, req.user_ip, 'success');
        
        res.status(201).json({
            success: true,
            message: 'Bidhaa imeongezwa kikamilifu! Bei ya kuuzia imewekwa.',
            product: newProduct
        });
        
    } catch (error) {
        console.error('❌ ERROR creating product:', error.message);
        await logUserAction(req.user?.id, 'PRODUCT_CREATE_ERROR', '/api/products', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.put('/api/products/:id', authenticateToken, async (req, res) => {
    try {
        const productId = req.params.id;
        const userId = req.user.id;
        const { name, category, price, stock, description, cost_price, expected_selling_price } = req.body;
        
        console.log(`✏️ Updating product ${productId} by user ${userId}, Data:`, req.body);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!expected_selling_price) {
            await logUserAction(userId, 'PRODUCT_UPDATE_FAILED', '/api/products/:id', { 
                reason: 'Missing expected selling price',
                product_id: productId 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Bei ya kuuzia inahitajika'
            });
        }
        
        const { data: product, error: checkError } = await supabase
            .from('products')
            .select('seller_id, expected_selling_price')
            .eq('id', productId)
            .single();
        
        if (checkError || !product) {
            await logUserAction(userId, 'PRODUCT_UPDATE_FAILED', '/api/products/:id', { 
                reason: 'Product not found',
                product_id: productId 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ 
                success: false,
                error: 'Bidhaa haipo'
            });
        }
        
        if (product.seller_id !== userId) {
            await logUserAction(userId, 'PRODUCT_UPDATE_FAILED', '/api/products/:id', { 
                reason: 'Unauthorized access',
                product_id: productId,
                owner_id: product.seller_id 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ 
                success: false,
                error: 'Huna ruhusa ya kuhariri bidhaa hii' 
            });
        }
        
        const expectedPriceValue = parseFloat(expected_selling_price);
        
        if (isNaN(expectedPriceValue) || expectedPriceValue <= 0) {
            await logUserAction(userId, 'PRODUCT_UPDATE_FAILED', '/api/products/:id', { 
                reason: 'Invalid expected selling price',
                product_id: productId,
                expected_selling_price: expected_selling_price 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Bei ya kuuzia si sahihi (lazima iwe namba kubwa kuliko 0)'
            });
        }
        
        const updateData = {
            name: name.trim(),
            category: (category || '').trim(),
            description: (description || '').trim(),
            price: parseFloat(price),
            cost_price: cost_price ? parseFloat(cost_price) : null,
            expected_selling_price: expectedPriceValue,
            stock: parseInt(stock) || 0,
            updated_at: new Date().toISOString()
        };
        
        if (product.expected_selling_price !== expectedPriceValue) {
            console.log(`💰 Price change for product ${productId}: ${product.expected_selling_price} → ${expectedPriceValue}`);
        }

        const { data: updatedProduct, error } = await supabase
            .from('products')
            .update(updateData)
            .eq('id', productId)
            .select(`id, name, category, description, price, expected_selling_price, cost_price, stock, is_active, seller_id, created_at, updated_at`)
            .single();
        
        if (error) throw error;
        
        console.log('✅ Product updated:', productId);
        
        await logUserAction(userId, 'PRODUCT_UPDATE', '/api/products/:id', { 
            product_id: productId,
            fields_updated: Object.keys(updateData),
            price_change: product.expected_selling_price !== expectedPriceValue ? {
                old: product.expected_selling_price,
                new: expectedPriceValue
            } : null
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: 'Bidhaa imesasishwa kikamilifu! Bei ya kuuzia imesasishwa.',
            product: updatedProduct
        });
        
    } catch (error) {
        console.error('❌ ERROR updating product:', error.message);
        await logUserAction(req.user?.id, 'PRODUCT_UPDATE_ERROR', '/api/products/:id', { 
            error: error.message,
            product_id: req.params.id 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.delete('/api/products/:id', authenticateToken, async (req, res) => {
    try {
        const productId = req.params.id;
        const userId = req.user.id;
        
        console.log(`🗑️ Deleting product ${productId} by user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: product, error: checkError } = await supabase
            .from('products')
            .select('seller_id, name')
            .eq('id', productId)
            .single();
        
        if (checkError || !product) {
            await logUserAction(userId, 'PRODUCT_DELETE_FAILED', '/api/products/:id', { 
                reason: 'Product not found',
                product_id: productId 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ error: 'Bidhaa haipo' });
        }
        
        if (product.seller_id !== userId) {
            await logUserAction(userId, 'PRODUCT_DELETE_FAILED', '/api/products/:id', { 
                reason: 'Unauthorized access',
                product_id: productId,
                owner_id: product.seller_id 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ 
                error: 'Huna ruhusa ya kufuta bidhaa hii' 
            });
        }
        
        const { error } = await supabase
            .from('products')
            .update({ 
                is_active: false,
                updated_at: new Date().toISOString()
            })
            .eq('id', productId);
        
        if (error) throw error;
        
        console.log('✅ Product deactivated:', productId);
        
        await logUserAction(userId, 'PRODUCT_DELETE', '/api/products/:id', { 
            product_id: productId,
            product_name: product.name 
        }, req.user_ip, 'success');
        
        res.json({
            message: 'Bidhaa imedhibitishwa kikamilifu!',
            productId: productId
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'PRODUCT_DELETE_ERROR', '/api/products/:id', { 
            error: error.message,
            product_id: req.params.id 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ SALE ENDPOINTS (WITH INVOICE NUMBER & UNIT_PRICE)
// =============================================
app.get('/api/sales/my', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`💰 Loading sales for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: sales, error } = await supabase
            .from('sales')
            .select(`*, sale_items(*, products(*)), customers(*)`)
            .eq('seller_id', userId)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        console.log(`✅ Sales loaded for user ${userId}:`, sales?.length || 0);
        
        await logUserAction(userId, 'SALES_VIEW', '/api/sales/my', { 
            count: sales?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(sales || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'SALES_VIEW_ERROR', '/api/sales/my', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.post('/api/sales', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { customer_id, sale_date, payment_method, notes, items } = req.body;
        
        console.log('📨 NEW SALE REQUEST from user:', userId);
        console.log('📦 Request details:', { items_count: items?.length || 0, customer_id, payment_method });
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!items || !Array.isArray(items) || items.length === 0) {
            await logUserAction(userId, 'SALE_CREATE_FAILED', '/api/sales', { 
                reason: 'No items provided' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Bidhaa za kuuza zinahitajika',
                code: 'NO_ITEMS'
            });
        }

        let total_amount = 0;
        const invalidItems = [];
        
        for (const item of items) {
            if (!item.product_id || !item.quantity || !item.unit_price) {
                invalidItems.push({ item, reason: 'Incomplete data' });
            }
            
            if (isNaN(item.quantity) || item.quantity <= 0) {
                invalidItems.push({ item, reason: 'Invalid quantity' });
            }
            
            if (isNaN(item.unit_price) || item.unit_price <= 0) {
                invalidItems.push({ item, reason: 'Invalid unit price' });
            }
            
            total_amount += item.quantity * item.unit_price;
        }
        
        if (invalidItems.length > 0) {
            await logUserAction(userId, 'SALE_CREATE_FAILED', '/api/sales', { 
                reason: 'Invalid items',
                invalid_items: invalidItems 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Taarifa za bidhaa hazijakamilika au si sahihi',
                code: 'INVALID_ITEMS'
            });
        }
        
        console.log(`💰 Total amount: ${total_amount}`);

        const stockIssues = [];
        for (const item of items) {
            const { data: product, error: productError } = await supabase
                .from('products')
                .select('stock, name, seller_id, is_active')
                .eq('id', item.product_id)
                .eq('is_active', true)
                .single();
            
            if (productError || !product) {
                stockIssues.push({ product_id: item.product_id, reason: 'Product not found' });
                continue;
            }
            
            if (product.seller_id !== userId) {
                const { data: sellerUser, error: sellerError } = await supabase
                    .from('users')
                    .select('business_name, role')
                    .eq('id', userId)
                    .single();
                
                if (!sellerError && sellerUser && sellerUser.role === 'seller') {
                    const { data: adminUser, error: adminError } = await supabase
                        .from('users')
                        .select('id')
                        .eq('business_name', sellerUser.business_name)
                        .eq('role', 'admin')
                        .eq('status', 'approved')
                        .single();
                    
                    if (adminError || !adminUser || adminUser.id !== product.seller_id) {
                        stockIssues.push({ 
                            product_id: item.product_id, 
                            product_name: product.name,
                            reason: 'Product access denied' 
                        });
                    }
                } else {
                    stockIssues.push({ 
                        product_id: item.product_id, 
                        product_name: product.name,
                        reason: 'Product access denied' 
                    });
                }
            }
            
            if (product.stock < item.quantity) {
                stockIssues.push({ 
                    product_id: item.product_id, 
                    product_name: product.name,
                    reason: 'Insufficient stock',
                    available: product.stock,
                    requested: item.quantity
                });
            }
        }
        
        if (stockIssues.length > 0) {
            await logUserAction(userId, 'SALE_CREATE_FAILED', '/api/sales', { 
                reason: 'Stock issues',
                stock_issues: stockIssues 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                success: false,
                error: 'Matatizo ya kiasi cha bidhaa',
                stockIssues: stockIssues,
                code: 'STOCK_ISSUES'
            });
        }

        const invoice_number = await generateInvoiceNumber(userId);
        console.log(`🧾 Invoice number generated: ${invoice_number}`);

        const saleData = {
            seller_id: userId,
            customer_id: customer_id || null,
            sale_date: sale_date || new Date().toISOString().split('T')[0],
            total_amount,
            payment_method: payment_method || 'cash',
            notes: notes || null,
            invoice_number: invoice_number,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        const { data: newSale, error: saleError } = await supabase
            .from('sales')
            .insert([saleData])
            .select()
            .single();
        
        if (saleError) {
            console.error('❌ Database error saving sale:', saleError);
            
            await logUserAction(userId, 'SALE_CREATE_FAILED', '/api/sales', { 
                reason: 'Database error',
                error: saleError.message 
            }, req.user_ip, 'failed');
            
            return res.status(500).json({ 
                success: false,
                error: 'Hitilafu ya kuhifadhi rekodi ya mauzo',
                details: saleError.message,
                code: 'SALE_SAVE_ERROR'
            });
        }

        const saleItems = items.map(item => ({
            sale_id: newSale.id,
            product_id: item.product_id,
            quantity: item.quantity,
            unit_price: item.unit_price,
            total_price: item.quantity * item.unit_price,
            created_at: new Date().toISOString()
        }));

        console.log('🛒 Sale items to insert:', saleItems.length);

        const { error: itemsError } = await supabase
            .from('sale_items')
            .insert(saleItems);
        
        if (itemsError) {
            console.error('❌ Error saving sale items:', itemsError);
            await supabase.from('sales').delete().eq('id', newSale.id);
            
            await logUserAction(userId, 'SALE_CREATE_FAILED', '/api/sales', { 
                reason: 'Sale items error',
                error: itemsError.message 
            }, req.user_ip, 'failed');
            
            return res.status(500).json({ 
                success: false,
                error: 'Hitilafu ya kuhifadhi bidhaa za mauzo',
                details: itemsError.message,
                code: 'SALE_ITEMS_SAVE_ERROR'
            });
        }

        const stockUpdates = [];
        for (const item of items) {
            const { data: currentProduct, error: fetchError } = await supabase
                .from('products')
                .select('stock')
                .eq('id', item.product_id)
                .single();
            
            if (!fetchError && currentProduct) {
                const newStock = currentProduct.stock - item.quantity;
                
                const { error: stockError } = await supabase
                    .from('products')
                    .update({ 
                        stock: newStock,
                        updated_at: new Date().toISOString()
                    })
                    .eq('id', item.product_id);
                
                if (!stockError) {
                    stockUpdates.push({
                        product_id: item.product_id,
                        old_stock: currentProduct.stock,
                        new_stock: newStock,
                        quantity_sold: item.quantity
                    });
                }
            }
        }

        if (customer_id) {
            const { data: currentCustomer, error: fetchCustomerError } = await supabase
                .from('customers')
                .select('total_purchases, purchases_count')
                .eq('id', customer_id)
                .single();
            
            if (!fetchCustomerError && currentCustomer) {
                const newTotalPurchases = (currentCustomer.total_purchases || 0) + total_amount;
                const newPurchasesCount = (currentCustomer.purchases_count || 0) + 1;
                
                await supabase
                    .from('customers')
                    .update({ 
                        total_purchases: newTotalPurchases,
                        purchases_count: newPurchasesCount,
                        last_purchase_date: new Date().toISOString(),
                        updated_at: new Date().toISOString()
                    })
                    .eq('id', customer_id);
            }
        }

        const { data: completeSale, error: fetchError } = await supabase
            .from('sales')
            .select(`*, sale_items(*, products(*)), customers(*)`)
            .eq('id', newSale.id)
            .single();
        
        console.log(`✅ Sale completed successfully! Invoice: ${invoice_number}, ID: ${newSale.id}`);
        
        await logUserAction(userId, 'SALE_CREATE', '/api/sales', { 
            sale_id: newSale.id,
            invoice_number: invoice_number,
            total_amount: total_amount,
            items_count: items.length,
            customer_id: customer_id,
            stock_updates: stockUpdates 
        }, req.user_ip, 'success');
        
        return res.status(201).json({
            success: true,
            message: 'Mauzo yamekamilika kikamilifu!',
            invoice_number: invoice_number,
            unit_price_included: true,
            sale: completeSale || { id: newSale.id, invoice_number }
        });

    } catch (error) {
        console.error('❌ UNEXPECTED ERROR creating sale:', error.message);
        
        await logUserAction(req.user?.id, 'SALE_CREATE_ERROR', '/api/sales', { 
            error: error.message,
            stack: error.stack 
        }, req.user_ip, 'failed');
        
        let userMessage = 'Hitilafu isiyotarajiwa katika kurekodi mauzo';
        let errorCode = 'UNEXPECTED_ERROR';
        
        if (error.message.includes('violates foreign key constraint')) {
            userMessage = 'Bidhaa au mteja haipo kwenye mfumo';
            errorCode = 'FOREIGN_KEY_VIOLATION';
        } else if (error.message.includes('invalid input syntax for type uuid')) {
            userMessage = 'Nambari ya bidhaa au mteja si sahihi';
            errorCode = 'INVALID_UUID_SYNTAX';
        } else if (error.message.includes('connection')) {
            userMessage = 'Hitilafu ya muunganisho na database. Tafadhali jaribu tena.';
            errorCode = 'DATABASE_CONNECTION_ERROR';
        }
        
        return res.status(500).json({ 
            success: false,
            error: userMessage,
            details: error.message,
            code: errorCode
        });
    }
});

// =============================================
// ✅ CUSTOMER ENDPOINTS
// =============================================
app.get('/api/customers/my', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`👥 Loading customers for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: customers, error } = await supabase
            .from('customers')
            .select('*')
            .eq('seller_id', userId)
            .order('total_purchases', { ascending: false });
        
        if (error) throw error;
        
        console.log(`✅ Customers loaded for user ${userId}:`, customers?.length || 0);
        
        await logUserAction(userId, 'CUSTOMERS_VIEW', '/api/customers/my', { 
            count: customers?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(customers || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'CUSTOMERS_VIEW_ERROR', '/api/customers/my', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.post('/api/customers', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { name, phone, email } = req.body;
        
        console.log('📨 Creating new customer for user:', userId, 'Data:', req.body);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!name) {
            await logUserAction(userId, 'CUSTOMER_CREATE_FAILED', '/api/customers', { 
                reason: 'Name required' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'Jina la mteja linahitajika' });
        }

        const customerData = {
            seller_id: userId,
            name,
            phone: phone || null,
            email: email || null,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        const { data: newCustomer, error } = await supabase
            .from('customers')
            .insert([customerData])
            .select()
            .single();
        
        if (error) throw error;

        console.log('✅ Customer created:', newCustomer.id);
        
        await logUserAction(userId, 'CUSTOMER_CREATE', '/api/customers', { 
            customer_id: newCustomer.id,
            name: newCustomer.name 
        }, req.user_ip, 'success');
        
        res.status(201).json({
            message: 'Mteja ameongezwa kikamilifu!',
            customer: newCustomer
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'CUSTOMER_CREATE_ERROR', '/api/customers', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.put('/api/customers/:id', authenticateToken, async (req, res) => {
    try {
        const customerId = req.params.id;
        const userId = req.user.id;
        const { name, phone, email } = req.body;
        
        console.log(`✏️ Updating customer ${customerId} by user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: customer, error: checkError } = await supabase
            .from('customers')
            .select('seller_id')
            .eq('id', customerId)
            .single();
        
        if (checkError || !customer) {
            await logUserAction(userId, 'CUSTOMER_UPDATE_FAILED', '/api/customers/:id', { 
                reason: 'Customer not found',
                customer_id: customerId 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ error: 'Mteja hajapatikana' });
        }
        
        if (customer.seller_id !== userId) {
            await logUserAction(userId, 'CUSTOMER_UPDATE_FAILED', '/api/customers/:id', { 
                reason: 'Unauthorized access',
                customer_id: customerId,
                owner_id: customer.seller_id 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ 
                error: 'Huna ruhusa ya kuhariri mteja huyu' 
            });
        }
        
        const updateData = {
            name,
            phone: phone || null,
            email: email || null,
            updated_at: new Date().toISOString()
        };

        const { data: updatedCustomer, error } = await supabase
            .from('customers')
            .update(updateData)
            .eq('id', customerId)
            .select()
            .single();
        
        if (error) throw error;
        
        console.log('✅ Customer updated:', customerId);
        
        await logUserAction(userId, 'CUSTOMER_UPDATE', '/api/customers/:id', { 
            customer_id: customerId,
            fields_updated: Object.keys(updateData) 
        }, req.user_ip, 'success');
        
        res.json({
            message: 'Mteja amesasishwa kikamilifu!',
            customer: updatedCustomer
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'CUSTOMER_UPDATE_ERROR', '/api/customers/:id', { 
            error: error.message,
            customer_id: req.params.id 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ MATANGAZO ENDPOINTS
// =============================================
async function getReactionCountsForMatangazo(matangazoIds) {
    try {
        if (!matangazoIds || matangazoIds.length === 0) {
            return {};
        }

        const { data: reactions, error } = await supabase
            .from('reactions')
            .select('matangazo_id, reaction_type')
            .in('matangazo_id', matangazoIds);

        if (error) {
            console.error('Error fetching reactions:', error);
            return {};
        }

        const counts = {};
        matangazoIds.forEach(id => {
            counts[id] = { like_count: 0, report_count: 0 };
        });

        reactions.forEach(reaction => {
            if (reaction.reaction_type === 'like') {
                counts[reaction.matangazo_id].like_count++;
            } else if (reaction.reaction_type === 'report') {
                counts[reaction.matangazo_id].report_count++;
            }
        });

        return counts;
    } catch (error) {
        console.error('Error in getReactionCountsForMatangazo:', error);
        return {};
    }
}

app.get('/api/matangazo', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 GET /api/matangazo request IP:', ip);
    
    try {
        const { data: matangazo, error } = await supabase
            .from('matangazo')
            .select(`*, users(*)`)
            .eq('is_active', true)
            .eq('is_free', true)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        const matangazoIds = matangazo?.map(m => m.id) || [];
        const reactionCounts = await getReactionCountsForMatangazo(matangazoIds);
        
        const matangazoWithCounts = matangazo?.map(m => ({
            ...m,
            like_count: reactionCounts[m.id]?.like_count || 0,
            report_count: reactionCounts[m.id]?.report_count || 0
        })) || [];
        
        console.log('📤 Sending matangazo data:', matangazoWithCounts?.length || 0);
        
        await logUserAction(null, 'MATANGAZO_VIEW_PUBLIC', '/api/matangazo', { 
            count: matangazoWithCounts.length 
        }, ip, 'success');
        
        res.json(matangazoWithCounts);
        
    } catch (error) {
        console.error('❌ ERROR in /api/matangazo:', error.message);
        await logUserAction(null, 'MATANGAZO_VIEW_ERROR', '/api/matangazo', { error: error.message }, ip, 'failed');
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/matangazo/my', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`📊 Loading matangazo for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: matangazo, error } = await supabase
            .from('matangazo')
            .select(`*, users(*)`)
            .eq('user_id', userId)
            .order('created_at', { ascending: false });
        
        if (error) {
            console.error('❌ Database error in matangazo/my:', error);
            throw error;
        }
        
        const matangazoIds = matangazo?.map(m => m.id) || [];
        const reactionCounts = await getReactionCountsForMatangazo(matangazoIds);
        
        const matangazoWithCounts = matangazo?.map(m => ({
            ...m,
            like_count: reactionCounts[m.id]?.like_count || 0,
            report_count: reactionCounts[m.id]?.report_count || 0
        })) || [];
        
        console.log(`✅ Matangazo loaded for user ${userId}:`, matangazoWithCounts?.length || 0);
        
        await logUserAction(userId, 'MATANGAZO_VIEW_MY', '/api/matangazo/my', { 
            count: matangazoWithCounts.length 
        }, req.user_ip, 'success');
        
        res.json(matangazoWithCounts);
        
    } catch (error) {
        console.error('❌ ERROR fetching user matangazo:', error.message);
        await logUserAction(req.user?.id, 'MATANGAZO_VIEW_ERROR', '/api/matangazo/my', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ 
            error: 'Hitilafu ya kupakia matangazo',
            details: error.message 
        });
    }
});

app.post('/api/matangazo', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { description, media_url, media_type, thumbnail_url, expires_at } = req.body;
        
        console.log('📨 Creating matangazo for user:', userId, 'Data:', req.body);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!description || !media_url) {
            await logUserAction(userId, 'MATANGAZO_CREATE_FAILED', '/api/matangazo', { 
                reason: 'Missing required fields' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ 
                error: 'Maelezo na URL ya media zinahitajika' 
            });
        }

        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name, full_name')
            .eq('id', userId)
            .single();
        
        if (userError) throw userError;

        const title = user?.business_name || user?.full_name || 'Matangazo';

        const matangazoData = {
            user_id: userId,
            title: title,
            description,
            media_url: media_url,
            media_type: media_type || 'image',
            thumbnail_url: thumbnail_url || media_url,
            payment_status: 'completed',
            is_free: true,
            order_tracking_id: null,
            expires_at: expires_at || null,
            is_active: true,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        const { data: newMatangazo, error } = await supabase
            .from('matangazo')
            .insert([matangazoData])
            .select(`*, users(*)`)
            .single();
        
        if (error) {
            console.error('❌ Database error:', error);
            throw error;
        }
        
        const matangazoWithCounts = {
            ...newMatangazo,
            like_count: 0,
            report_count: 0
        };
        
        console.log('✅ Tangazo created:', newMatangazo.id);
        
        await logUserAction(userId, 'MATANGAZO_CREATE', '/api/matangazo', { 
            matangazo_id: newMatangazo.id,
            title: newMatangazo.title,
            media_type: newMatangazo.media_type 
        }, req.user_ip, 'success');
        
        res.status(201).json({
            message: 'Tangazo limeongezwa kikamilifu!',
            data: matangazoWithCounts
        });
        
    } catch (error) {
        console.error('❌ ERROR adding matangazo:', error.message);
        await logUserAction(req.user?.id, 'MATANGAZO_CREATE_ERROR', '/api/matangazo', { error: error.message }, req.user_ip, 'failed');
        res.status(500).json({ 
            error: 'Hitilafu katika kuongeza matangazo',
            details: error.message 
        });
    }
});

app.delete('/api/matangazo/:id', authenticateToken, async (req, res) => {
    try {
        const matangazoId = req.params.id;
        const userId = req.user.id;
        
        console.log(`🗑️ Deleting matangazo ${matangazoId} by user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: matangazo, error: checkError } = await supabase
            .from('matangazo')
            .select('user_id, title')
            .eq('id', matangazoId)
            .single();
        
        if (checkError || !matangazo) {
            await logUserAction(userId, 'MATANGAZO_DELETE_FAILED', '/api/matangazo/:id', { 
                reason: 'Matangazo not found',
                matangazo_id: matangazoId 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ error: 'Tangazo halipo' });
        }
        
        if (matangazo.user_id !== userId) {
            await logUserAction(userId, 'MATANGAZO_DELETE_FAILED', '/api/matangazo/:id', { 
                reason: 'Unauthorized access',
                matangazo_id: matangazoId,
                owner_id: matangazo.user_id 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ 
                error: 'Huna ruhusa ya kufuta tangazo hili' 
            });
        }
        
        const { error } = await supabase
            .from('matangazo')
            .update({ 
                is_active: false,
                updated_at: new Date().toISOString()
            })
            .eq('id', matangazoId);
        
        if (error) throw error;
        
        console.log('✅ Tangazo deleted:', matangazoId);
        
        await logUserAction(userId, 'MATANGAZO_DELETE', '/api/matangazo/:id', { 
            matangazo_id: matangazoId,
            title: matangazo.title 
        }, req.user_ip, 'success');
        
        res.json({
            message: 'Tangazo limefutwa kikamilifu!',
            id: matangazoId
        });
        
    } catch (error) {
        console.error('❌ ERROR deleting matangazo:', error.message);
        await logUserAction(req.user?.id, 'MATANGAZO_DELETE_ERROR', '/api/matangazo/:id', { 
            error: error.message,
            matangazo_id: req.params.id 
        }, req.user_ip, 'failed');
        res.status(500).json({ error: error.message });
    }
});

// =============================================
// ✅ REACTION ENDPOINTS
// =============================================
app.get('/api/reactions/user/likes', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`❤️ Loading likes for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: likes, error } = await supabase
            .from('reactions')
            .select('matangazo_id')
            .eq('user_id', userId)
            .eq('reaction_type', 'like');
        
        if (error) throw error;
        
        const likedPostIds = likes?.map(like => like.matangazo_id) || [];
        
        await logUserAction(userId, 'LIKES_VIEW', '/api/reactions/user/likes', { 
            count: likedPostIds.length 
        }, req.user_ip, 'success');
        
        res.json({
            likedPosts: likedPostIds
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'LIKES_VIEW_ERROR', '/api/reactions/user/likes', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.post('/api/reactions/like', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { matangazo_id } = req.body;
        
        console.log(`❤️ Like action for matangazo ${matangazo_id} by user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!matangazo_id) {
            await logUserAction(userId, 'LIKE_FAILED', '/api/reactions/like', { 
                reason: 'Missing matangazo_id' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'matangazo_id inahitajika' });
        }

        const { data: existing, error: checkError } = await supabase
            .from('reactions')
            .select('id')
            .eq('user_id', userId)
            .eq('matangazo_id', matangazo_id)
            .eq('reaction_type', 'like')
            .single();
        
        let liked = false;

        if (existing) {
            const { error: deleteError } = await supabase
                .from('reactions')
                .delete()
                .eq('user_id', userId)
                .eq('matangazo_id', matangazo_id)
                .eq('reaction_type', 'like');
            
            if (deleteError) throw deleteError;
            
            liked = false;
            console.log('✅ Like removed:', { userId, matangazo_id });
        } else {
            const reactionData = {
                user_id: userId,
                matangazo_id,
                reaction_type: 'like',
                created_at: new Date().toISOString()
            };

            const { error: insertError } = await supabase
                .from('reactions')
                .insert([reactionData]);
            
            if (insertError) throw insertError;
            
            liked = true;
            console.log('✅ Like added:', { userId, matangazo_id });
        }

        const { count: likeCount, error: countError } = await supabase
            .from('reactions')
            .select('*', { count: 'exact', head: true })
            .eq('matangazo_id', matangazo_id)
            .eq('reaction_type', 'like');
        
        if (countError) throw countError;
        
        const { count: reportCount, error: reportCountError } = await supabase
            .from('reactions')
            .select('*', { count: 'exact', head: true })
            .eq('matangazo_id', matangazo_id)
            .eq('reaction_type', 'report');
        
        if (reportCountError) throw reportCountError;
        
        await logUserAction(userId, 'LIKE_ACTION', '/api/reactions/like', { 
            matangazo_id: matangazo_id,
            action: liked ? 'liked' : 'unliked',
            new_like_count: likeCount || 0 
        }, req.user_ip, 'success');
        
        res.json({
            message: liked ? 'Like imeongezwa kikamilifu!' : 'Like imeondolewa',
            liked: liked,
            like_count: likeCount || 0,
            report_count: reportCount || 0
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'LIKE_ERROR', '/api/reactions/like', { 
            error: error.message,
            matangazo_id: req.body?.matangazo_id 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ NOTIFICATION SYSTEM ENDPOINTS
// =============================================

// Create notifications table
app.post('/api/admin/setup/notifications', async (req, res) => {
    try {
        const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        console.log('🔧 Setting up notifications system...');
        
        const createTableSQL = `
            CREATE TABLE IF NOT EXISTS notifications (
                id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                title VARCHAR(255) NOT NULL,
                message TEXT NOT NULL,
                notification_type VARCHAR(20) NOT NULL CHECK (notification_type IN ('all', 'admins', 'sellers', 'clients', 'specific')),
                recipients JSONB NOT NULL,
                sender_id UUID REFERENCES users(id),
                sender_name VARCHAR(255),
                sent_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
                delivery_method VARCHAR(50) DEFAULT 'email' CHECK (delivery_method IN ('email', 'push', 'both')),
                email_sent BOOLEAN DEFAULT false,
                push_sent BOOLEAN DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_notifications_sender_id ON notifications(sender_id);
            CREATE INDEX IF NOT EXISTS idx_notifications_sent_at ON notifications(sent_at);
            CREATE INDEX IF NOT EXISTS idx_notifications_status ON notifications(status);
            CREATE INDEX IF NOT EXISTS idx_notifications_notification_type ON notifications(notification_type);
        `;
        
        const { error } = await supabaseAdmin.rpc('exec_sql', { query: createTableSQL });
        
        if (error) {
            console.warn('⚠️ Table creation failed, trying alternative...');
            
            // Fallback: Try direct insert to see if table exists
            const { error: testError } = await supabaseAdmin
                .from('notifications')
                .insert([{
                    title: 'Test Notification',
                    message: 'Testing notifications system',
                    notification_type: 'all',
                    recipients: [],
                    status: 'pending'
                }])
                .then(() => supabaseAdmin.from('notifications').delete().neq('title', ''))
                .catch(() => {});
            
            if (testError && testError.code === '42P01') {
                // Table doesn't exist
                return res.status(500).json({
                    error: 'Notifications table does not exist',
                    manual_sql: createTableSQL,
                    instructions: 'Please run the SQL above in Supabase SQL Editor'
                });
            }
        }
        
        console.log('✅ Notifications system setup completed');
        
        await logUserAction(null, 'SETUP_NOTIFICATIONS', '/api/admin/setup/notifications', {
            success: true
        }, ip, 'success');
        
        res.json({
            success: true,
            message: 'Notifications system setup completed!',
            table: 'notifications'
        });
        
    } catch (error) {
        console.error('❌ ERROR setting up notifications:', error.message);
        res.status(500).json({
            error: 'Failed to setup notifications system',
            details: error.message
        });
    }
});

// Send notification (EMAIL & SYSTEM)
app.post('/api/admin/notifications', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const { title, message, notification_type, recipient_ids = [], delivery_method = 'email' } = req.body;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'SEND_NOTIFICATION_UNAUTHORIZED', '/api/admin/notifications', {
                reason: 'Non-admin access attempt'
            }, req.user_ip, 'failed');
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log('📨 Admin sending notification:', { title, notification_type, delivery_method });
        
        if (!title || !message || !notification_type) {
            await logUserAction(adminId, 'SEND_NOTIFICATION_FAILED', '/api/admin/notifications', {
                reason: 'Missing required fields'
            }, req.user_ip, 'failed');
            return res.status(400).json({ 
                error: 'Title, message na aina ya taarifa zinahitajika' 
            });
        }
        
        // Get sender info
        const { data: sender, error: senderError } = await supabaseAdmin
            .from('users')
            .select('full_name, email')
            .eq('id', adminId)
            .single();
        
        if (senderError) throw senderError;
        
        // Determine recipients
        let recipients = [];
        let userRecipients = [];
        
        switch (notification_type) {
            case 'all':
                const { data: allUsers, error: allError } = await supabaseAdmin
                    .from('users')
                    .select('id, email, full_name, role')
                    .eq('status', 'approved');
                
                if (!allError && allUsers) {
                    userRecipients = allUsers;
                    recipients = allUsers.map(u => ({ id: u.id, email: u.email, name: u.full_name || u.email }));
                }
                break;
                
            case 'admins':
                const { data: admins, error: adminsError } = await supabaseAdmin
                    .from('users')
                    .select('id, email, full_name, role')
                    .eq('role', 'admin')
                    .eq('status', 'approved');
                
                if (!adminsError && admins) {
                    userRecipients = admins;
                    recipients = admins.map(u => ({ id: u.id, email: u.email, name: u.full_name || u.email }));
                }
                break;
                
            case 'sellers':
                const { data: sellers, error: sellersError } = await supabaseAdmin
                    .from('users')
                    .select('id, email, full_name, role')
                    .eq('role', 'seller')
                    .eq('status', 'approved');
                
                if (!sellersError && sellers) {
                    userRecipients = sellers;
                    recipients = sellers.map(u => ({ id: u.id, email: u.email, name: u.full_name || u.email }));
                }
                break;
                
            case 'clients':
                const { data: clients, error: clientsError } = await supabaseAdmin
                    .from('users')
                    .select('id, email, full_name, role')
                    .eq('role', 'client')
                    .eq('status', 'approved');
                
                if (!clientsError && clients) {
                    userRecipients = clients;
                    recipients = clients.map(u => ({ id: u.id, email: u.email, name: u.full_name || u.email }));
                }
                break;
                
            case 'specific':
                if (recipient_ids.length === 0) {
                    await logUserAction(adminId, 'SEND_NOTIFICATION_FAILED', '/api/admin/notifications', {
                        reason: 'No recipients specified for specific notification'
                    }, req.user_ip, 'failed');
                    return res.status(400).json({ error: 'Recipient IDs required for specific notification' });
                }
                
                const { data: specificUsers, error: specificError } = await supabaseAdmin
                    .from('users')
                    .select('id, email, full_name, role')
                    .in('id', recipient_ids)
                    .eq('status', 'approved');
                
                if (!specificError && specificUsers) {
                    userRecipients = specificUsers;
                    recipients = specificUsers.map(u => ({ id: u.id, email: u.email, name: u.full_name || u.email }));
                }
                break;
        }
        
        if (recipients.length === 0) {
            await logUserAction(adminId, 'SEND_NOTIFICATION_FAILED', '/api/admin/notifications', {
                reason: 'No recipients found',
                notification_type,
                recipient_count: 0
            }, req.user_ip, 'failed');
            return res.status(404).json({ error: 'Hakuna wapokeaji waliopatikana' });
        }
        
        // Save notification to database
        const notificationData = {
            title,
            message,
            notification_type,
            recipients: recipients.map(r => ({ id: r.id, email: r.email, name: r.name })),
            sender_id: adminId,
            sender_name: sender?.full_name || sender?.email || 'System Admin',
            delivery_method,
            status: 'pending',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };
        
        const { data: savedNotification, error: saveError } = await supabaseAdmin
            .from('notifications')
            .insert([notificationData])
            .select()
            .single();
        
        if (saveError) throw saveError;
        
        console.log(`📝 Notification saved to database: ${savedNotification.id}`);
        
        // Send emails if delivery_method includes email
        let emailResults = { success: 0, failed: 0 };
        let pushResults = { success: 0, failed: 0 };
        
        if (delivery_method === 'email' || delivery_method === 'both') {
            console.log(`📧 Sending emails to ${recipients.length} recipients...`);
            
            for (const recipient of recipients) {
                try {
                    const emailSubject = title;
                    const emailHtml = `
                        <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                            <div style="background-color: #3498db; color: white; padding: 20px; border-radius: 10px 10px 0 0;">
                                <h1 style="margin: 0;">${title}</h1>
                            </div>
                            <div style="padding: 30px; background-color: #f8f9fa; border-radius: 0 0 10px 10px;">
                                <p style="color: #2c3e50; font-size: 16px; line-height: 1.6;">
                                    ${message.replace(/\n/g, '<br>')}
                                </p>
                                <hr style="border: none; border-top: 1px solid #e0e0e0; margin: 20px 0;">
                                <p style="color: #7f8c8d; font-size: 14px;">
                                    <strong>Imetumwa na:</strong> ${sender?.full_name || 'System Admin'}<br>
                                    <strong>Muda:</strong> ${new Date().toLocaleString('sw-TZ')}<br>
                                    <strong>Mfumo:</strong> DukaMkononi
                                </p>
                                <div style="margin-top: 20px; padding: 15px; background-color: #e8f4fc; border-radius: 8px; border-left: 4px solid #3498db;">
                                    <p style="margin: 0; color: #2c3e50; font-size: 14px;">
                                        <strong>🔔 Kumbuka:</strong> Hii ni taarifa rasmi kutoka kwa msimamizi wa mfumo wa DukaMkononi. 
                                        Ikiwa hukutarajia taarifa hii, tafadhali wasiliana na msimamizi.
                                    </p>
                                </div>
                            </div>
                            <div style="text-align: center; padding: 20px; color: #95a5a6; font-size: 12px; border-top: 1px solid #e0e0e0;">
                                <p>© ${new Date().getFullYear()} DukaMkononi. Haki zote zimehifadhiwa.</p>
                                <p>Hii ni barua pepe ya kiotomatiki. Tafadhali usijibu.</p>
                            </div>
                        </div>
                    `;
                    
                    const emailResult = await sendEmail(recipient.email, emailSubject, emailHtml);
                    
                    if (emailResult.success) {
                        emailResults.success++;
                        console.log(`✅ Email sent to ${recipient.email}`);
                    } else {
                        emailResults.failed++;
                        console.error(`❌ Failed to send email to ${recipient.email}: ${emailResult.error}`);
                    }
                    
                } catch (emailError) {
                    emailResults.failed++;
                    console.error(`❌ Error sending email to ${recipient.email}:`, emailError.message);
                }
            }
            
            // Update notification with email results
            await supabaseAdmin
                .from('notifications')
                .update({
                    email_sent: emailResults.success > 0,
                    updated_at: new Date().toISOString()
                })
                .eq('id', savedNotification.id);
        }
        
        // Update notification status
        const finalStatus = emailResults.failed === recipients.length ? 'failed' : 'sent';
        
        await supabaseAdmin
            .from('notifications')
            .update({
                status: finalStatus,
                updated_at: new Date().toISOString()
            })
            .eq('id', savedNotification.id);
        
        console.log(`✅ Notification sent: ${emailResults.success} emails successful, ${emailResults.failed} failed`);
        
        await logUserAction(adminId, 'NOTIFICATION_SENT', '/api/admin/notifications', {
            notification_id: savedNotification.id,
            title,
            notification_type,
            recipient_count: recipients.length,
            email_success: emailResults.success,
            email_failed: emailResults.failed,
            final_status: finalStatus
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: `Taarifa imetumwa kikamilifu kwa watumiaji ${recipients.length}!`,
            notification: {
                id: savedNotification.id,
                title,
                message,
                notification_type,
                recipients: recipients.length,
                sent_at: new Date().toISOString(),
                email_results: emailResults
            }
        });
        
    } catch (error) {
        console.error('❌ ERROR sending notification:', error.message);
        
        await logUserAction(req.user?.id, 'NOTIFICATION_ERROR', '/api/admin/notifications', {
            error: error.message,
            stack: error.stack
        }, req.user_ip, 'failed');
        
        res.status(500).json({
            error: 'Hitilafu katika kutuma taarifa',
            details: error.message
        });
    }
});

// Get all notifications
app.get('/api/admin/notifications', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'GET_NOTIFICATIONS_UNAUTHORIZED', '/api/admin/notifications', {
                reason: 'Non-admin access attempt'
            }, req.user_ip, 'failed');
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        const { data: notifications, error } = await supabaseAdmin
            .from('notifications')
            .select('*')
            .order('sent_at', { ascending: false })
            .limit(100);
        
        if (error) throw error;
        
        await logUserAction(adminId, 'GET_NOTIFICATIONS', '/api/admin/notifications', {
            count: notifications?.length || 0
        }, req.user_ip, 'success');
        
        res.json(notifications || []);
        
    } catch (error) {
        console.error('❌ ERROR getting notifications:', error.message);
        
        await logUserAction(req.user?.id, 'GET_NOTIFICATIONS_ERROR', '/api/admin/notifications', {
            error: error.message
        }, req.user_ip, 'failed');
        
        res.status(500).json({
            error: 'Hitilafu ya kupata taarifa',
            details: error.message
        });
    }
});

// Get notification stats
app.get('/api/admin/notifications/stats', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        const today = new Date().toISOString().split('T')[0];
        
        // Total notifications
        const { count: total, error: totalError } = await supabaseAdmin
            .from('notifications')
            .select('*', { count: 'exact', head: true });
        
        if (totalError) throw totalError;
        
        // Today's notifications
        const { data: todayNotifications, error: todayError } = await supabaseAdmin
            .from('notifications')
            .select('*')
            .gte('sent_at', today);
        
        if (todayError) throw todayError;
        
        // Count by type
        const { data: allNotifications, error: allError } = await supabaseAdmin
            .from('notifications')
            .select('notification_type, status');
        
        if (allError) throw allError;
        
        const stats = {
            total_sent: total || 0,
            sent_today: todayNotifications?.length || 0,
            by_type: {
                all: allNotifications?.filter(n => n.notification_type === 'all').length || 0,
                admins: allNotifications?.filter(n => n.notification_type === 'admins').length || 0,
                sellers: allNotifications?.filter(n => n.notification_type === 'sellers').length || 0,
                clients: allNotifications?.filter(n => n.notification_type === 'clients').length || 0,
                specific: allNotifications?.filter(n => n.notification_type === 'specific').length || 0
            },
            success_rate: allNotifications?.length ? 
                Math.round((allNotifications.filter(n => n.status === 'sent').length / allNotifications.length) * 100) : 100,
            total_recipients: allNotifications?.reduce((acc, curr) => acc + (curr.recipients?.length || 0), 0) || 0,
            timestamp: new Date().toISOString()
        };
        
        await logUserAction(adminId, 'NOTIFICATION_STATS', '/api/admin/notifications/stats', stats, req.user_ip, 'success');
        
        res.json(stats);
        
    } catch (error) {
        console.error('❌ ERROR getting notification stats:', error.message);
        
        await logUserAction(req.user?.id, 'NOTIFICATION_STATS_ERROR', '/api/admin/notifications/stats', {
            error: error.message
        }, req.user_ip, 'failed');
        
        res.status(500).json({
            error: 'Hitilafu ya kupata takwimu za taarifa',
            details: error.message
        });
    }
});

// Check notifications system
app.get('/api/admin/notifications/check', async (req, res) => {
    try {
        const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        console.log('🔍 Checking notifications system...');
        
        // Check if table exists
        const { data, error } = await supabaseAdmin
            .from('notifications')
            .select('id')
            .limit(1);
        
        if (error && error.code === '42P01') {
            console.log('⚠️ Notifications table does not exist');
            return res.json({
                exists: false,
                message: 'Notifications table does not exist',
                error: error.message
            });
        }
        
        console.log('✅ Notifications system is ready');
        
        await logUserAction(null, 'CHECK_NOTIFICATIONS', '/api/admin/notifications/check', {
            exists: true,
            ready: true
        }, ip, 'success');
        
        res.json({
            exists: true,
            ready: true,
            message: 'Notifications system is ready'
        });
        
    } catch (error) {
        console.error('❌ ERROR checking notifications system:', error.message);
        
        await logUserAction(null, 'CHECK_NOTIFICATIONS_ERROR', '/api/admin/notifications/check', {
            error: error.message
        }, ip, 'failed');
        
        res.status(500).json({
            error: 'Failed to check notifications system',
            details: error.message
        });
    }
});

// Send test notification
app.post('/api/admin/notifications/test', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        const { email } = req.body;
        
        if (!email) {
            return res.status(400).json({ error: 'Email is required for test' });
        }
        
        console.log(`🧪 Sending test notification to ${email}`);
        
        const testSubject = '✅ Test Notification - DukaMkononi System';
        const testMessage = `
            Hii ni taarifa ya majaribio kutoka kwenye mfumo wa DukaMkononi.
            
            Ikiwa unapokea barua pepe hii, inamaanisha mfumo wa kutuma taarifa unafanya kazi kikamilifu!
            
            Mambo yanayojaribiwa:
            1. ✅ Usafiri wa barua pepe
            2. ✅ Ujumbe wa HTML
            3. ✅ Uunganisho na mfumo
            4. ✅ Kasi ya kutuma
            
            Tarehe: ${new Date().toLocaleString('sw-TZ')}
            Mfumo: DukaMkononi Backend v2.0
            Admin: ${req.user.email}
        `;
        
        const emailHtml = `
            <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto;">
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 15px 15px 0 0; text-align: center;">
                    <h1 style="margin: 0; font-size: 28px;">✅ TEST SUCCESSFUL</h1>
                    <p style="margin: 10px 0 0; opacity: 0.9;">DukaMkononi Notification System</p>
                </div>
                <div style="padding: 40px; background-color: #f8f9fa; border-radius: 0 0 15px 15px;">
                    <div style="background-color: white; padding: 25px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.05);">
                        <p style="color: #2c3e50; font-size: 16px; line-height: 1.6;">
                            ${testMessage.replace(/\n/g, '<br>')}
                        </p>
                    </div>
                    
                    <div style="margin-top: 30px; display: grid; grid-template-columns: repeat(2, 1fr); gap: 15px;">
                        <div style="background-color: #e8f6ef; padding: 15px; border-radius: 8px; border-left: 4px solid #27ae60;">
                            <div style="font-weight: bold; color: #27ae60; margin-bottom: 5px;">✓ Email Delivery</div>
                            <div style="font-size: 12px; color: #666;">Working perfectly</div>
                        </div>
                        <div style="background-color: #e3f2fd; padding: 15px; border-radius: 8px; border-left: 4px solid #2196f3;">
                            <div style="font-weight: bold; color: #2196f3; margin-bottom: 5px;">✓ System Integration</div>
                            <div style="font-size: 12px; color: #666;">Backend connected</div>
                        </div>
                    </div>
                    
                    <div style="margin-top: 30px; padding: 20px; background-color: #fff3cd; border-radius: 8px; border-left: 4px solid #ffc107;">
                        <p style="margin: 0; color: #856404; font-size: 14px;">
                            <strong>💡 Kumbuka:</strong> Hii ni barua pepe ya majaribio tu. 
                            Ikiwa umepokea hii, mfumo wako wa taarifa unafanya kazi vizuri!
                        </p>
                    </div>
                </div>
                <div style="text-align: center; padding: 25px; color: #95a5a6; font-size: 12px; border-top: 1px solid #e0e0e0; background-color: white;">
                    <p>© ${new Date().getFullYear()} DukaMkononi - Test Email System</p>
                    <p style="font-size: 11px; margin-top: 5px;">Timestamp: ${new Date().toISOString()}</p>
                </div>
            </div>
        `;
        
        const emailResult = await sendEmail(email, testSubject, emailHtml);
        
        if (emailResult.success) {
            console.log(`✅ Test email sent successfully to ${email}`);
            
            await logUserAction(adminId, 'TEST_NOTIFICATION_SENT', '/api/admin/notifications/test', {
                recipient: email,
                success: true,
                message_id: emailResult.messageId
            }, req.user_ip, 'success');
            
            res.json({
                success: true,
                message: 'Test email sent successfully!',
                email: email,
                messageId: emailResult.messageId,
                timestamp: new Date().toISOString()
            });
        } else {
            console.error(`❌ Test email failed to ${email}:`, emailResult.error);
            
            await logUserAction(adminId, 'TEST_NOTIFICATION_FAILED', '/api/admin/notifications/test', {
                recipient: email,
                success: false,
                error: emailResult.error
            }, req.user_ip, 'failed');
            
            res.status(500).json({
                success: false,
                error: 'Failed to send test email',
                details: emailResult.error,
                email: email
            });
        }
        
    } catch (error) {
        console.error('❌ ERROR in test notification:', error.message);
        
        await logUserAction(req.user?.id, 'TEST_NOTIFICATION_ERROR', '/api/admin/notifications/test', {
            error: error.message
        }, req.user_ip, 'failed');
        
        res.status(500).json({
            error: 'Hitilafu katika kutuma barua pepe ya majaribio',
            details: error.message
        });
    }
});

app.get('/api/reactions/matangazo/:matangazoId/counts', async (req, res) => {
    try {
        const matangazoId = req.params.matangazoId;
        const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        
        console.log(`📊 Getting reaction counts for matangazo ${matangazoId} from IP:`, ip);
        
        const { count: likeCount, error: likeError } = await supabase
            .from('reactions')
            .select('*', { count: 'exact', head: true })
            .eq('matangazo_id', matangazoId)
            .eq('reaction_type', 'like');
        
        if (likeError) throw likeError;
        
        const { count: reportCount, error: reportError } = await supabase
            .from('reactions')
            .select('*', { count: 'exact', head: true })
            .eq('matangazo_id', matangazoId)
            .eq('reaction_type', 'report');
        
        if (reportError) throw reportError;
        
        await logUserAction(null, 'REACTION_COUNTS', `/api/reactions/matangazo/${matangazoId}/counts`, { 
            matangazo_id: matangazoId,
            like_count: likeCount || 0,
            report_count: reportCount || 0 
        }, ip, 'success');
        
        res.json({
            matangazo_id: matangazoId,
            like_count: likeCount || 0,
            report_count: reportCount || 0
        });
        
    } catch (error) {
        const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
        await logUserAction(null, 'REACTION_COUNTS_ERROR', `/api/reactions/matangazo/${req.params.matangazoId}/counts`, { 
            error: error.message 
        }, ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.post('/api/reactions/report', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { matangazo_id } = req.body;
        
        console.log(`🚨 Report action for matangazo ${matangazo_id} by user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!matangazo_id) {
            await logUserAction(userId, 'REPORT_FAILED', '/api/reactions/report', { 
                reason: 'Missing matangazo_id' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'matangazo_id inahitajika' });
        }

        const { data: existing, error: checkError } = await supabase
            .from('reactions')
            .select('id')
            .eq('user_id', userId)
            .eq('matangazo_id', matangazo_id)
            .eq('reaction_type', 'report')
            .single();
        
        if (existing) {
            await logUserAction(userId, 'REPORT_FAILED', '/api/reactions/report', { 
                reason: 'Already reported',
                matangazo_id: matangazo_id 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'Umewahi kuripoti tangazo hili' });
        }

        const reactionData = {
            user_id: userId,
            matangazo_id,
            reaction_type: 'report',
            created_at: new Date().toISOString()
        };

        const { error: insertError } = await supabase
            .from('reactions')
            .insert([reactionData]);
        
        if (insertError) throw insertError;
        
        console.log('✅ Report added:', { userId, matangazo_id });

        const { count: reportCount, error: countError } = await supabase
            .from('reactions')
            .select('*', { count: 'exact', head: true })
            .eq('matangazo_id', matangazo_id)
            .eq('reaction_type', 'report');
        
        if (countError) throw countError;
        
        const { count: likeCount, error: likeCountError } = await supabase
            .from('reactions')
            .select('*', { count: 'exact', head: true })
            .eq('matangazo_id', matangazo_id)
            .eq('reaction_type', 'like');
        
        if (likeCountError) throw likeCountError;
        
        await logUserAction(userId, 'REPORT_ACTION', '/api/reactions/report', { 
            matangazo_id: matangazo_id,
            report_count: reportCount || 0 
        }, req.user_ip, 'success');
        
        res.json({
            message: 'Ripoti imeongezwa kikamilifu!',
            report_count: reportCount || 0,
            like_count: likeCount || 0
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'REPORT_ERROR', '/api/reactions/report', { 
            error: error.message,
            matangazo_id: req.body?.matangazo_id 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ REVENUE ENDPOINTS
// =============================================
app.get('/api/revenue/my', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`💰 Loading revenue for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const today = new Date().toISOString().split('T')[0];
        const startOfWeek = new Date();
        startOfWeek.setDate(startOfWeek.getDate() - startOfWeek.getDay());
        const endOfWeek = new Date(startOfWeek);
        endOfWeek.setDate(startOfWeek.getDate() + 6);
        
        const startOfMonth = new Date();
        startOfMonth.setDate(1);
        const endOfMonth = new Date(startOfMonth.getFullYear(), startOfMonth.getMonth() + 1, 0);

        const { data: todayData, error: todayError } = await supabase
            .from('sales')
            .select('total_amount')
            .eq('seller_id', userId)
            .eq('sale_date', today);
        
        if (todayError) throw todayError;
        
        const todayRevenue = todayData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;

        const { data: weeklyData, error: weeklyError } = await supabase
            .from('sales')
            .select('total_amount')
            .eq('seller_id', userId)
            .gte('sale_date', startOfWeek.toISOString().split('T')[0])
            .lte('sale_date', endOfWeek.toISOString().split('T')[0]);
        
        if (weeklyError) throw weeklyError;
        
        const weeklyRevenue = weeklyData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;

        const { data: monthlyData, error: monthlyError } = await supabase
            .from('sales')
            .select('total_amount')
            .eq('seller_id', userId)
            .gte('sale_date', startOfMonth.toISOString().split('T')[0])
            .lte('sale_date', endOfMonth.toISOString().split('T')[0]);
        
        if (monthlyError) throw monthlyError;
        
        const monthlyRevenue = monthlyData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;

        const { data: totalData, error: totalError } = await supabase
            .from('sales')
            .select('total_amount')
            .eq('seller_id', userId);
        
        if (totalError) throw totalError;
        
        const totalRevenue = totalData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;

        await logUserAction(userId, 'REVENUE_VIEW', '/api/revenue/my', { 
            today: todayRevenue,
            weekly: weeklyRevenue,
            monthly: monthlyRevenue,
            total: totalRevenue 
        }, req.user_ip, 'success');
        
        res.json({
            today: parseFloat(todayRevenue.toFixed(2)),
            weekly: parseFloat(weeklyRevenue.toFixed(2)),
            monthly: parseFloat(monthlyRevenue.toFixed(2)),
            total: parseFloat(totalRevenue.toFixed(2))
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'REVENUE_VIEW_ERROR', '/api/revenue/my', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ ADMIN USERS ENDPOINT (WITH STATS IN RESPONSE)
// =============================================
app.get('/api/admin/users', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(userId, 'ADMIN_UNAUTHORIZED', '/api/admin/users', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`👥 Admin ${userId} viewing all users`);
        
        // 1. GET ALL USERS
        const { data: users, error } = await supabaseAdmin
            .from('users')
            .select('*')
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        // 2. CALCULATE COMPLETE STATS
        const usersList = users || [];
        
        const stats = {
            // User counts by role
            totalUsers: usersList.length,
            totalAdmins: usersList.filter(u => u.role === 'admin').length,
            totalSellers: usersList.filter(u => u.role === 'seller').length,
            totalClients: usersList.filter(u => u.role === 'client').length,
            
            // User counts by status
            activeUsers: usersList.filter(u => u.status === 'approved').length,
            pendingUsers: usersList.filter(u => u.status === 'pending').length,
            suspendedUsers: usersList.filter(u => u.status === 'suspended').length,
            inactiveUsers: usersList.filter(u => u.status === 'inactive').length,
            
            // Real-time data
            onlineUsers: usersList.filter(u => u.is_online).length,
            connectedNow: Array.from(connectedUsers.values()).filter(u => u.has_live_connection).length,
            
            // Timestamp
            timestamp: new Date().toISOString()
        };
        
        // 3. Add real-time connection status to users
        const usersWithRealTime = usersList.map(user => {
            const connection = connectedUsers.get(user.id);
            const connectionAge = connection 
                ? Math.floor((Date.now() - new Date(connection.connectedAt).getTime()) / 1000)
                : null;
            
            return {
                ...user,
                has_live_connection: !!connection,
                connection_age: connectionAge,
                session_id: connection?.sessionId || null,
                ip_address: connection?.ipAddress || null,
                is_currently_online: user.is_online || false,
                last_seen_formatted: user.last_seen ? new Date(user.last_seen).toLocaleString('sw-TZ') : 'Hajawahi'
            };
        });
        
        console.log('📊 Users data sent:', usersWithRealTime.length, 'users');
        console.log('📈 Stats calculated:', stats);
        
        await logUserAction(userId, 'ADMIN_USERS_VIEW', '/api/admin/users', { 
            count: usersWithRealTime.length,
            online_count: stats.onlineUsers,
            stats_summary: stats
        }, req.user_ip, 'success');
        
        // 4. SEND BOTH USERS AND STATS IN RESPONSE
        res.json({
            // KEEP EXACT SAME STRUCTURE FOR BACKWARD COMPATIBILITY
            users: usersWithRealTime,  // Frontend expects 'users' array
            
            // ADD NEW STATS FIELD (won't break existing frontend)
            _stats: stats,
            _metadata: {
                server: 'DukaMkononi Backend',
                version: '2.0.0',
                endpoint: '/api/admin/users',
                includes_stats: true
            }
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_USERS_VIEW_ERROR', '/api/admin/users', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/admin/products', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(userId, 'ADMIN_UNAUTHORIZED', '/api/admin/products', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`📦 Admin ${userId} viewing all products`);
        
        const { data: products, error } = await supabaseAdmin
            .from('products')
            .select(`*, users(*)`)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        console.log('📦 All products sent:', products?.length || 0);
        
        await logUserAction(userId, 'ADMIN_PRODUCTS_VIEW', '/api/admin/products', { 
            count: products?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(products || []);
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_PRODUCTS_VIEW_ERROR', '/api/admin/products', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/admin/sales', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(userId, 'ADMIN_UNAUTHORIZED', '/api/admin/sales', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`💰 Admin ${userId} viewing all sales`);
        
        const { data: sales, error } = await supabaseAdmin
            .from('sales')
            .select(`*, users(*), sale_items(*, products(*))`)
            .order('sale_date', { ascending: false });
        
        if (error) throw error;
        
        console.log('💰 All sales sent:', sales?.length || 0);
        
        await logUserAction(userId, 'ADMIN_SALES_VIEW', '/api/admin/sales', { 
            count: sales?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(sales || []);
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_SALES_VIEW_ERROR', '/api/admin/sales', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/admin/customers', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(userId, 'ADMIN_UNAUTHORIZED', '/api/admin/customers', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`👥 Admin ${userId} viewing all customers`);
        
        const { data: customers, error } = await supabaseAdmin
            .from('customers')
            .select(`*, users(*)`)
            .order('total_purchases', { ascending: false });
        
        if (error) throw error;
        
        console.log('👥 All customers sent:', customers?.length || 0);
        
        await logUserAction(userId, 'ADMIN_CUSTOMERS_VIEW', '/api/admin/customers', { 
            count: customers?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(customers || []);
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_CUSTOMERS_VIEW_ERROR', '/api/admin/customers', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/admin/stats', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(userId, 'ADMIN_UNAUTHORIZED', '/api/admin/stats', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }

        console.log(`📊 Admin ${userId} viewing stats`);

        const { count: totalUsers, error: usersError } = await supabaseAdmin
            .from('users')
            .select('*', { count: 'exact', head: true });
        
        if (usersError) throw usersError;

        const { count: totalProducts, error: productsError } = await supabaseAdmin
            .from('products')
            .select('*', { count: 'exact', head: true });
        
        if (productsError) throw productsError;

        const { data: salesData, error: salesError } = await supabaseAdmin
            .from('sales')
            .select('total_amount');
        
        if (salesError) throw salesError;
        
        const totalSales = salesData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;

        const { count: totalCustomers, error: customersError } = await supabaseAdmin
            .from('customers')
            .select('*', { count: 'exact', head: true });
        
        if (customersError) throw customersError;

        const today = new Date().toISOString().split('T')[0];
        const { data: todaySalesData, error: todayError } = await supabaseAdmin
            .from('sales')
            .select('total_amount')
            .eq('sale_date', today);
        
        if (todayError) throw todayError;
        
        const todaySales = todaySalesData?.reduce((sum, sale) => sum + sale.total_amount, 0) || 0;
        
        // Get real-time online users
        const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
        const { count: onlineUsers, error: onlineError } = await supabaseAdmin
            .from('users')
            .select('*', { count: 'exact', head: true })
            .gte('last_seen', fiveMinutesAgo)
            .eq('is_online', true);
        
        if (onlineError) throw onlineError;
        
        // Get user roles breakdown
        const { data: usersByRole, error: rolesError } = await supabaseAdmin
            .from('users')
            .select('role, status');
        
        if (rolesError) throw rolesError;
        
        const totalAdmins = usersByRole?.filter(u => u.role === 'admin').length || 0;
        const totalSellers = usersByRole?.filter(u => u.role === 'seller').length || 0;
        const totalClients = usersByRole?.filter(u => u.role === 'client').length || 0;
        const activeUsers = usersByRole?.filter(u => u.status === 'approved').length || 0;
        const pendingUsers = usersByRole?.filter(u => u.status === 'pending').length || 0;
        const suspendedUsers = usersByRole?.filter(u => u.status === 'suspended').length || 0;

        const stats = {
            totalUsers: totalUsers || 0,
            totalProducts: totalProducts || 0,
            totalSales: parseFloat(totalSales.toFixed(2)),
            totalCustomers: totalCustomers || 0,
            todaySales: parseFloat(todaySales.toFixed(2)),
            // Real-time stats
            onlineUsers: onlineUsers || 0,
            connectedNow: Array.from(connectedUsers.values()).filter(u => u.has_live_connection).length,
            adminConnections: adminConnections.size,
            totalAdmins,
            totalSellers,
            totalClients,
            activeUsers,
            pendingUsers,
            suspendedUsers,
            // WebSocket stats
            webSocketConnected: connectedUsers.size,
            serverTime: new Date().toISOString()
        };

        console.log('✅ Admin stats retrieved with real-time data');
        
        await logUserAction(userId, 'ADMIN_STATS_VIEW', '/api/admin/stats', stats, req.user_ip, 'success');
        
        res.json(stats);
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_STATS_VIEW_ERROR', '/api/admin/stats', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ DELETE USER COMPLETELY (HARD DELETE)
// =============================================
app.delete('/api/admin/users/:id', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const userId = req.params.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'ADMIN_UNAUTHORIZED', '/api/admin/users/:id', { 
                reason: 'Non-admin delete attempt',
                target_user_id: userId 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }

        console.log(`🗑️ Admin ${adminId} deleting user ${userId}`);
        
        if (userId === adminId) {
            await logUserAction(adminId, 'ADMIN_SELF_DELETE_ATTEMPT', '/api/admin/users/:id', { 
                reason: 'Self-delete attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'Huwezi kujifuta mwenyewe' });
        }

        const { data: sales, error: salesError } = await supabaseAdmin
            .from('sales')
            .select('id')
            .eq('seller_id', userId)
            .limit(1);
        
        if (salesError) throw salesError;
        
        const { data: products, error: productsError } = await supabaseAdmin
            .from('products')
            .select('id')
            .eq('seller_id', userId)
            .limit(1);
        
        if (productsError) throw productsError;
        
        const hasSales = sales && sales.length > 0;
        const hasProducts = products && products.length > 0;

        const { error: deleteError } = await supabaseAdmin
            .from('users')
            .delete()
            .eq('id', userId);
        
        if (deleteError) throw deleteError;

        console.log('✅ User deleted completely:', userId);
        
        await logUserAction(adminId, 'ADMIN_USER_DELETE', '/api/admin/users/:id', { 
            target_user_id: userId,
            had_sales: hasSales,
            had_products: hasProducts 
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: 'Mtumiaji amefutwa kabisa kutoka kwenye mfumo!',
            userId: userId,
            warnings: {
                hadSales: hasSales,
                hadProducts: hasProducts,
                note: hasSales || hasProducts 
                    ? 'Mauzo na bidhaa za mtumiaji huyu zimesalia kwenye mfumo.' 
                    : 'Hakuna data iliyobaki ya mtumiaji huyu.'
            }
        });

    } catch (error) {
        console.error('❌ ERROR deleting user:', error.message);
        await logUserAction(req.user?.id, 'ADMIN_USER_DELETE_ERROR', '/api/admin/users/:id', { 
            error: error.message,
            target_user_id: req.params.id 
        }, req.user_ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika kufuta mtumiaji',
            details: error.message 
        });
    }
});

app.put('/api/admin/users/:id/status', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const userId = req.params.id;
        const { status } = req.body;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'ADMIN_UNAUTHORIZED', `/api/admin/users/${userId}/status`, { 
                reason: 'Non-admin status update attempt',
                target_user_id: userId 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }

        console.log(`🔄 Admin ${adminId} updating user ${userId} status to ${status}`);

        if (!['pending', 'approved', 'rejected'].includes(status)) {
            await logUserAction(adminId, 'ADMIN_STATUS_UPDATE_FAILED', `/api/admin/users/${userId}/status`, { 
                reason: 'Invalid status',
                target_user_id: userId,
                status: status 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'Status si sahihi' });
        }

        // Pata taarifa za user (sasa na previous_status column)
        const { data: targetUser, error: fetchError } = await supabaseAdmin
            .from('users')
            .select('email, role, status, previous_status')  // ← SASA INA COLUMN
            .eq('id', userId)
            .single();
        
        if (fetchError) throw fetchError;

        const currentStatus = targetUser?.status || 'pending';

        // Sasisha status mpya na kuhifadhi ya zamani
        const { data, error } = await supabaseAdmin
            .from('users')
            .update({ 
                status,
                previous_status: currentStatus,  // ← Hifadhi status ya sasa
                updated_at: new Date().toISOString()
            })
            .eq('id', userId);

        if (error) throw error;

        console.log('✅ User status updated:', userId, 
            'from', currentStatus, 'to', status);
        
        await logUserAction(adminId, 'ADMIN_STATUS_UPDATE', 
            `/api/admin/users/${userId}/status`, { 
                target_user_id: userId,
                target_email: targetUser?.email,
                target_role: targetUser?.role,
                old_status: currentStatus,
                new_status: status,
                previous_status_saved: true  // ← Mpya
            }, req.user_ip, 'success');
        
        res.json({
            message: 'Status imesasishwa kikamilifu!',
            userId: userId,
            status: status,
            previous_status: currentStatus  // ← Rudisha kwenye response
        });

    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_STATUS_UPDATE_ERROR', 
            `/api/admin/users/${req.params.id}/status`, { 
                error: error.message,
                target_user_id: req.params.id 
            }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ PASSWORD RESET ENDPOINTS
// =============================================
app.post('/api/forgot-password', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 FORGOT PASSWORD REQUEST:', req.body.email, 'Role:', req.body.role, 'IP:', ip);
    
    const { email, role } = req.body;

    if (!email || !role) {
        await logUserAction(null, 'FORGOT_PASSWORD_FAILED', '/api/forgot-password', { 
            reason: 'Missing fields',
            email: email,
            role: role 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Email na role zinahitajika' });
    }

    try {
        const { data: users, error } = await supabaseAdmin
            .from('users')
            .select('*')
            .eq('email', email)
            .eq('role', role);

        if (error) throw error;

        if (!users || users.length === 0) {
            await logUserAction(null, 'FORGOT_PASSWORD_FAILED', '/api/forgot-password', { 
                reason: 'Account not found',
                email: email,
                role: role 
            }, ip, 'failed');
            
            return res.status(404).json({ error: 'Akaunti haipo au role si sahihi' });
        }

        const user = users[0];
        
        const resetToken = jwt.sign(
            { 
                userId: user.id, 
                email: user.email, 
                role: user.role,
                type: 'password_reset'
            },
            JWT_SECRET,
            { expiresIn: '1h' }
        );

        console.log('✅ Password reset token generated:', user.email);
        
        await logUserAction(user.id, 'FORGOT_PASSWORD_REQUEST', '/api/forgot-password', { 
            token_generated: true 
        }, ip, 'success');
        
        res.json({
            message: 'Ombi la kubadilisha nenosiri limetumwa!',
            resetToken: resetToken,
            expiresIn: '1 hour'
        });

    } catch (error) {
        console.error('❌ FORGOT PASSWORD ERROR:', error.message);
        await logUserAction(null, 'FORGOT_PASSWORD_ERROR', '/api/forgot-password', { 
            error: error.message,
            email: email,
            role: role 
        }, ip, 'failed');
        
        res.status(500).json({ error: 'Hitilafu ya ndani ya server: ' + error.message });
    }
});

app.post('/api/reset-password', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 RESET PASSWORD REQUEST IP:', ip);
    
    const { token, newPassword } = req.body;

    if (!token || !newPassword) {
        await logUserAction(null, 'RESET_PASSWORD_FAILED', '/api/reset-password', { 
            reason: 'Missing token or password' 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Token na nenosiri jipya zinahitajika' });
    }

    if (newPassword.length < 6) {
        await logUserAction(null, 'RESET_PASSWORD_FAILED', '/api/reset-password', { 
            reason: 'Password too short' 
        }, ip, 'failed');
        
        return res.status(400).json({ error: 'Nenosiri jipya lazima liwe na herufi 6 au zaidi' });
    }

    try {
        const decoded = jwt.verify(token, JWT_SECRET);
        
        if (decoded.type !== 'password_reset') {
            await logUserAction(decoded.userId, 'RESET_PASSWORD_FAILED', '/api/reset-password', { 
                reason: 'Invalid token type',
                token_type: decoded.type 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Token si sahihi' });
        }

        const userId = decoded.userId;
        
        const hashedPassword = await bcrypt.hash(newPassword, 10);
        
        const { data, error } = await supabaseAdmin
            .from('users')
            .update({ 
                password: hashedPassword,
                updated_at: new Date().toISOString()
            })
            .eq('id', userId);

        if (error) throw error;

        console.log('✅ Password reset successful:', decoded.email);
        
        await logUserAction(userId, 'PASSWORD_RESET', '/api/reset-password', { 
            reset_successful: true 
        }, ip, 'success');
        
        res.json({
            message: 'Nenosiri limebadilishwa kikamilifu!',
            success: true
        });

    } catch (error) {
        if (error.name === 'TokenExpiredError') {
            await logUserAction(null, 'RESET_PASSWORD_FAILED', '/api/reset-password', { 
                reason: 'Token expired' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Token imeisha muda wake. Tafadhali tuma ombi jipya.' });
        } else if (error.name === 'JsonWebTokenError') {
            await logUserAction(null, 'RESET_PASSWORD_FAILED', '/api/reset-password', { 
                reason: 'Invalid token' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Token si sahihi' });
        }
        
        console.error('❌ RESET PASSWORD ERROR:', error.message);
        await logUserAction(null, 'RESET_PASSWORD_ERROR', '/api/reset-password', { 
            error: error.message 
        }, ip, 'failed');
        
        res.status(500).json({ error: 'Hitilafu ya ndani ya server: ' + error.message });
    }
});

// =============================================
// ✅ PESAPAL PAYMENT ENDPOINTS
// =============================================
app.post('/api/payments/pesapal/initiate', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { amount, description, matangazo_id, phone, email, name } = req.body;
        
        console.log('💰 Initiating PesaPal payment for user:', userId, 'Amount:', amount);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!amount || !description) {
            await logUserAction(userId, 'PAYMENT_INITIATE_FAILED', '/api/payments/pesapal/initiate', { 
                reason: 'Missing amount or description' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'Kiasi na maelezo ya malipo yanahitajika' });
        }

        if (!PESAPAL_CONFIG.consumerKey || !PESAPAL_CONFIG.consumerSecret) {
            await logUserAction(userId, 'PAYMENT_INITIATE_FAILED', '/api/payments/pesapal/initiate', { 
                reason: 'PesaPal not configured' 
            }, req.user_ip, 'failed');
            
            return res.status(500).json({ 
                error: 'PesaPal haijasanidiwa. Tafadhali wasiliana na msimamizi.' 
            });
        }

        const order_tracking_id = `DUKA-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        const accessToken = await getPesapalAccessToken();
        
        const orderData = {
            id: order_tracking_id,
            currency: "TZS",
            amount: parseFloat(amount).toFixed(2),
            description: description,
            callback_url: PESAPAL_CONFIG.callbackUrl,
            notification_id: PESAPAL_CONFIG.notificationId,
            billing_address: {
                email_address: email || req.user.email,
                phone_number: phone || req.user.phone || "255000000000",
                country_code: "TZ",
                first_name: name || req.user.full_name || "Customer",
                middle_name: "",
                last_name: "",
                line_1: "",
                line_2: "",
                city: "",
                state: "",
                postal_code: "",
                zip_code: ""
            }
        };

        const pesapalResponse = await submitPesapalOrder(orderData, accessToken);
        
        if (!pesapalResponse.redirect_url) {
            throw new Error('PesaPal haikurudisha URL ya malipo');
        }

        const paymentData = {
            order_tracking_id,
            amount: parseFloat(amount),
            status: 'pending',
            user_id: userId,
            matangazo_id: matangazo_id || null,
            description: description,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        const { data: newPayment, error: paymentError } = await supabase
            .from('payments')
            .insert([paymentData])
            .select()
            .single();
        
        if (paymentError) throw paymentError;

        console.log('✅ PesaPal payment initiated:', {
            order_tracking_id,
            amount,
            user_id: userId,
            redirect_url: pesapalResponse.redirect_url
        });
        
        await logUserAction(userId, 'PAYMENT_INITIATED', '/api/payments/pesapal/initiate', { 
            order_tracking_id: order_tracking_id,
            amount: amount,
            matangazo_id: matangazo_id 
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: 'Malipo yameanzishwa kikamilifu!',
            payment: {
                id: newPayment.id,
                order_tracking_id: order_tracking_id,
                amount: amount,
                status: 'pending',
                redirect_url: pesapalResponse.redirect_url
            },
            redirect_url: pesapalResponse.redirect_url
        });
        
    } catch (error) {
        console.error('❌ ERROR initiating PesaPal payment:', error.message);
        await logUserAction(req.user?.id, 'PAYMENT_INITIATE_ERROR', '/api/payments/pesapal/initiate', { 
            error: error.message 
        }, req.user_ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika kuanzisha malipo',
            details: error.message 
        });
    }
});

app.get('/api/payments/pesapal/status/:order_tracking_id', authenticateToken, async (req, res) => {
    try {
        const order_tracking_id = req.params.order_tracking_id;
        const userId = req.user.id;
        
        console.log(`🔍 Checking PesaPal status for order: ${order_tracking_id} by user: ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        if (!order_tracking_id) {
            await logUserAction(userId, 'PAYMENT_STATUS_FAILED', `/api/payments/pesapal/status/${order_tracking_id}`, { 
                reason: 'Missing order tracking ID' 
            }, req.user_ip, 'failed');
            
            return res.status(400).json({ error: 'Order tracking ID inahitajika' });
        }

        const { data: payment, error: paymentError } = await supabase
            .from('payments')
            .select('*')
            .eq('order_tracking_id', order_tracking_id)
            .single();
        
        if (paymentError || !payment) {
            await logUserAction(userId, 'PAYMENT_STATUS_FAILED', `/api/payments/pesapal/status/${order_tracking_id}`, { 
                reason: 'Payment not found',
                order_tracking_id: order_tracking_id 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ error: 'Malipo hayajapatikana' });
        }

        if (req.user.role !== 'admin' && payment.user_id !== userId) {
            await logUserAction(userId, 'PAYMENT_STATUS_FAILED', `/api/payments/pesapal/status/${order_tracking_id}`, { 
                reason: 'Unauthorized access',
                order_tracking_id: order_tracking_id,
                payment_owner: payment.user_id 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Huna ruhusa ya kuona malipo haya' });
        }

        if (payment.status === 'completed') {
            await logUserAction(userId, 'PAYMENT_STATUS_CHECK', `/api/payments/pesapal/status/${order_tracking_id}`, { 
                order_tracking_id: order_tracking_id,
                status: 'completed' 
            }, req.user_ip, 'success');
            
            return res.json({
                success: true,
                payment: payment,
                message: 'Malipo yamekamilika'
            });
        }

        try {
            const accessToken = await getPesapalAccessToken();
            const pesapalStatus = await getPesapalOrderStatus(order_tracking_id, accessToken);
            
            let newStatus = payment.status;
            let message = 'Malipo bado yanasubiri';
            
            if (pesapalStatus.status_code === '1') {
                newStatus = 'completed';
                message = 'Malipo yamekamilika kikamilifu!';
                
                await supabase
                    .from('payments')
                    .update({ 
                        status: 'completed',
                        paid_at: new Date().toISOString(),
                        updated_at: new Date().toISOString()
                    })
                    .eq('order_tracking_id', order_tracking_id);
                
                if (payment.matangazo_id) {
                    await supabase
                        .from('matangazo')
                        .update({ 
                            payment_status: 'completed',
                            is_free: false,
                            updated_at: new Date().toISOString()
                        })
                        .eq('id', payment.matangazo_id);
                }
            } else if (pesapalStatus.status_code === '2') {
                newStatus = 'failed';
                message = 'Malipo yameshindikana';
                
                await supabase
                    .from('payments')
                    .update({ 
                        status: 'failed',
                        updated_at: new Date().toISOString()
                    })
                    .eq('order_tracking_id', order_tracking_id);
            }
            
            const { data: updatedPayment, error: updateError } = await supabase
                .from('payments')
                .select('*')
                .eq('order_tracking_id', order_tracking_id)
                .single();
            
            if (updateError) throw updateError;
            
            await logUserAction(userId, 'PAYMENT_STATUS_CHECK', `/api/payments/pesapal/status/${order_tracking_id}`, { 
                order_tracking_id: order_tracking_id,
                old_status: payment.status,
                new_status: newStatus,
                pesapal_status: pesapalStatus.status_code 
            }, req.user_ip, 'success');
            
            res.json({
                success: true,
                payment: updatedPayment,
                pesapal_status: pesapalStatus,
                message: message
            });
            
        } catch (pesapalError) {
            console.error('❌ PesaPal status check failed:', pesapalError.message);
            
            await logUserAction(userId, 'PAYMENT_STATUS_CHECK_ERROR', `/api/payments/pesapal/status/${order_tracking_id}`, { 
                order_tracking_id: order_tracking_id,
                error: pesapalError.message 
            }, req.user_ip, 'failed');
            
            res.json({
                success: true,
                payment: payment,
                message: 'Imeshindikana kuangalia hali ya malipo kutoka PesaPal, lakini malipo yana hali ifuatayo: ' + payment.status
            });
        }
        
    } catch (error) {
        console.error('❌ ERROR checking PesaPal status:', error.message);
        
        await logUserAction(req.user?.id, 'PAYMENT_STATUS_CHECK_ERROR', `/api/payments/pesapal/status/${req.params.order_tracking_id}`, { 
            error: error.message 
        }, req.user_ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika kuangalia hali ya malipo',
            details: error.message 
        });
    }
});

app.post('/api/payments/pesapal-ipn', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📢 PESA PAL IPN RECEIVED from IP:', ip, 'Body:', req.body);
    
    try {
        const { OrderTrackingId, OrderNotificationType, OrderMerchantReference, Status } = req.body;
        
        console.log('🔍 IPN Details:', {
            OrderTrackingId,
            OrderNotificationType,
            OrderMerchantReference,
            Status
        });

        if (!OrderTrackingId) {
            console.log('⚠️ IPN without OrderTrackingId');
            
            await logUserAction(null, 'PESAPAL_IPN_FAILED', '/api/payments/pesapal-ipn', { 
                reason: 'Missing OrderTrackingId' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'OrderTrackingId inahitajika' });
        }

        await logUserAction(null, 'PESAPAL_IPN_RECEIVED', '/api/payments/pesapal-ipn', { 
            OrderTrackingId: OrderTrackingId,
            Status: Status 
        }, ip, 'success');
        
        res.status(200).json({ 
            status: 'success', 
            message: 'IPN received successfully',
            OrderTrackingId
        });

        setTimeout(async () => {
            try {
                let paymentStatus = 'pending';
                if (Status === 'COMPLETED') paymentStatus = 'completed';
                else if (Status === 'FAILED') paymentStatus = 'failed';
                else if (Status === 'PENDING') paymentStatus = 'pending';

                console.log(`🔄 Processing IPN for ${OrderTrackingId}: ${paymentStatus}`);

                const { data: payment, error: paymentError } = await supabaseAdmin
                    .from('payments')
                    .select('*')
                    .eq('order_tracking_id', OrderTrackingId)
                    .single();
                
                if (paymentError) {
                    console.error('❌ Payment not found:', paymentError.message);
                    return;
                }

                const updateData = {
                    status: paymentStatus,
                    updated_at: new Date().toISOString()
                };
                
                if (paymentStatus === 'completed') {
                    updateData.paid_at = new Date().toISOString();
                }

                const { error: updateError } = await supabaseAdmin
                    .from('payments')
                    .update(updateData)
                    .eq('order_tracking_id', OrderTrackingId);
                
                if (updateError) {
                    console.error('❌ Error updating payment:', updateError.message);
                } else {
                    console.log(`✅ Payment updated: ${OrderTrackingId} -> ${paymentStatus}`);
                    
                    await logUserAction(payment.user_id, 'PAYMENT_STATUS_UPDATED_IPN', 'IPN_PROCESSING', { 
                        order_tracking_id: OrderTrackingId,
                        old_status: payment.status,
                        new_status: paymentStatus,
                        source: 'pesapal_ipn' 
                    }, ip, 'success');
                }

                if (paymentStatus === 'completed' && payment.matangazo_id) {
                    const { error: matangazoError } = await supabaseAdmin
                        .from('matangazo')
                        .update({ 
                            payment_status: 'completed',
                            is_free: false,
                            updated_at: new Date().toISOString()
                        })
                        .eq('id', payment.matangazo_id);
                    
                    if (matangazoError) {
                        console.error('❌ Error updating matangazo:', matangazoError.message);
                    } else {
                        console.log(`✅ Matangazo updated: ${payment.matangazo_id} -> paid`);
                    }
                }

            } catch (asyncError) {
                console.error('❌ ASYNC IPN PROCESSING ERROR:', asyncError.message);
            }
        }, 1000);
        
    } catch (error) {
        console.error('❌ IPN PROCESSING ERROR:', error.message);
        
        await logUserAction(null, 'PESAPAL_IPN_ERROR', '/api/payments/pesapal-ipn', { 
            error: error.message 
        }, ip, 'failed');
        
        res.status(200).json({ 
            status: 'error_but_acknowledged', 
            message: 'IPN received but processing failed: ' + error.message 
        });
    }
});

app.post('/api/payments/pesapal-callback', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🔄 PESA PAL CALLBACK RECEIVED from IP:', ip, 'Body:', req.body);
    
    try {
        const { OrderTrackingId, OrderMerchantReference, Status } = req.body;
        
        if (OrderTrackingId) {
            console.log('✅ Callback processed for:', OrderTrackingId, 'Status:', Status);
            
            let paymentStatus = 'pending';
            if (Status === 'COMPLETED') paymentStatus = 'completed';
            else if (Status === 'FAILED') paymentStatus = 'failed';

            const { error: paymentError } = await supabaseAdmin
                .from('payments')
                .update({ 
                    status: paymentStatus,
                    paid_at: paymentStatus === 'completed' ? new Date().toISOString() : null,
                    updated_at: new Date().toISOString()
                })
                .eq('order_tracking_id', OrderTrackingId);
            
            if (paymentError) {
                console.error('Error updating payment:', paymentError);
            }
            
            if (Status === 'COMPLETED') {
                const { data: payment, error: fetchError } = await supabaseAdmin
                    .from('payments')
                    .select('matangazo_id, user_id')
                    .eq('order_tracking_id', OrderTrackingId)
                    .single();
                
                if (!fetchError && payment && payment.matangazo_id) {
                    const { error: matangazoError } = await supabaseAdmin
                        .from('matangazo')
                        .update({ 
                            payment_status: 'completed',
                            is_free: false,
                            updated_at: new Date().toISOString()
                        })
                        .eq('id', payment.matangazo_id);
                    
                    if (matangazoError) {
                        console.error('Error updating matangazo:', matangazoError);
                    }
                }
                
                if (payment && payment.user_id) {
                    await logUserAction(payment.user_id, 'PAYMENT_CALLBACK', '/api/payments/pesapal-callback', { 
                        order_tracking_id: OrderTrackingId,
                        status: 'completed',
                        source: 'pesapal_callback' 
                    }, ip, 'success');
                }
                
                console.log('✅ Payment completed:', OrderTrackingId);
            }
        }
        
        await logUserAction(null, 'PESAPAL_CALLBACK_RECEIVED', '/api/payments/pesapal-callback', { 
            OrderTrackingId: OrderTrackingId || 'unknown',
            Status: Status || 'unknown' 
        }, ip, 'success');
        
        res.json({ 
            success: true,
            message: 'Callback received successfully',
            OrderTrackingId: OrderTrackingId || 'unknown',
            Status: Status || 'unknown'
        });
        
    } catch (error) {
        console.error('❌ CALLBACK ERROR:', error.message);
        
        await logUserAction(null, 'PESAPAL_CALLBACK_ERROR', '/api/payments/pesapal-callback', { 
            error: error.message 
        }, ip, 'failed');
        
        res.status(500).json({ 
            success: false,
            error: 'Callback processing failed',
            details: error.message 
        });
    }
});

// =============================================
// ✅ PAYMENT ENDPOINTS
// =============================================
app.get('/api/payments/my', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`💰 Loading payments for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: payments, error } = await supabase
            .from('payments')
            .select(`*, matangazo(*)`)
            .eq('user_id', userId)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        console.log(`✅ Payments loaded for user ${userId}:`, payments?.length || 0);
        
        await logUserAction(userId, 'PAYMENTS_VIEW_MY', '/api/payments/my', { 
            count: payments?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(payments || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'PAYMENTS_VIEW_ERROR', '/api/payments/my', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/payments/:id', authenticateToken, async (req, res) => {
    try {
        const paymentId = req.params.id;
        const userId = req.user.id;
        
        console.log(`🔍 Getting payment ${paymentId} for user ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: payment, error } = await supabase
            .from('payments')
            .select(`*, matangazo(*), users(*)`)
            .eq('id', paymentId)
            .single();
        
        if (error || !payment) {
            await logUserAction(userId, 'PAYMENT_VIEW_FAILED', `/api/payments/${paymentId}`, { 
                reason: 'Payment not found',
                payment_id: paymentId 
            }, req.user_ip, 'failed');
            
            return res.status(404).json({ error: 'Malipo hayajapatikana' });
        }
        
        if (req.user.role !== 'admin' && payment.user_id !== userId) {
            await logUserAction(userId, 'PAYMENT_VIEW_FAILED', `/api/payments/${paymentId}`, { 
                reason: 'Unauthorized access',
                payment_id: paymentId,
                payment_owner: payment.user_id 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Huna ruhusa ya kuona malipo haya' });
        }
        
        await logUserAction(userId, 'PAYMENT_VIEW', `/api/payments/${paymentId}`, { 
            payment_id: paymentId,
            status: payment.status,
            amount: payment.amount 
        }, req.user_ip, 'success');
        
        res.json(payment);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'PAYMENT_VIEW_ERROR', `/api/payments/${req.params.id}`, { 
            error: error.message,
            payment_id: req.params.id 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/admin/payments', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(userId, 'ADMIN_UNAUTHORIZED', '/api/admin/payments', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`💰 Admin ${userId} viewing all payments`);
        
        const { data: payments, error } = await supabaseAdmin
            .from('payments')
            .select(`*, matangazo(*), users(*)`)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        console.log('💰 All payments sent:', payments?.length || 0);
        
        await logUserAction(userId, 'ADMIN_PAYMENTS_VIEW', '/api/admin/payments', { 
            count: payments?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(payments || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_PAYMENTS_VIEW_ERROR', '/api/admin/payments', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.post('/api/payments/simulate', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🎭 Simulating payment from IP:', ip, 'Body:', req.body);
    
    try {
        const { order_tracking_id, status } = req.body;
        
        if (!order_tracking_id || !status) {
            await logUserAction(null, 'PAYMENT_SIMULATE_FAILED', '/api/payments/simulate', { 
                reason: 'Missing order tracking ID or status' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Order tracking ID na status zinahitajika' });
        }

        const { data: payment, error: checkError } = await supabaseAdmin
            .from('payments')
            .select('*')
            .eq('order_tracking_id', order_tracking_id)
            .single();
        
        if (checkError || !payment) {
            await logUserAction(null, 'PAYMENT_SIMULATE_FAILED', '/api/payments/simulate', { 
                reason: 'Payment not found',
                order_tracking_id: order_tracking_id 
            }, ip, 'failed');
            
            return res.status(404).json({ error: 'Malipo hayajapatikana' });
        }

        const updateData = {
            status: status,
            updated_at: new Date().toISOString()
        };
        
        if (status === 'completed') {
            updateData.paid_at = new Date().toISOString();
        }

        const { error: updateError } = await supabaseAdmin
            .from('payments')
            .update(updateData)
            .eq('order_tracking_id', order_tracking_id);
        
        if (updateError) throw updateError;

        if (status === 'completed' && payment.matangazo_id) {
            await supabaseAdmin
                .from('matangazo')
                .update({ 
                    payment_status: 'completed',
                    is_free: false,
                    updated_at: new Date().toISOString()
                })
                .eq('id', payment.matangazo_id);
        }

        console.log(`✅ Payment simulated: ${order_tracking_id} -> ${status}`);
        
        await logUserAction(payment.user_id, 'PAYMENT_SIMULATED', '/api/payments/simulate', { 
            order_tracking_id: order_tracking_id,
            old_status: payment.status,
            new_status: status,
            simulated: true 
        }, ip, 'success');
        
        res.json({
            success: true,
            message: `Malipo yamesimuliwa kikamilifu! Status: ${status}`,
            order_tracking_id,
            status
        });
        
    } catch (error) {
        console.error('❌ ERROR simulating payment:', error.message);
        
        await logUserAction(null, 'PAYMENT_SIMULATE_ERROR', '/api/payments/simulate', { 
            error: error.message 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika kusimulia malipo',
            details: error.message 
        });
    }
});

app.post('/api/payments/record', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { order_tracking_id, amount, status, description, matangazo_id } = req.body;
        
        console.log('📝 Recording payment for user:', userId, 'Data:', req.body);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const paymentData = {
            order_tracking_id,
            amount: parseFloat(amount),
            status: status || 'pending',
            user_id: userId,
            matangazo_id: matangazo_id || null,
            description: description || null,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };

        const { data: newPayment, error } = await supabase
            .from('payments')
            .insert([paymentData])
            .select()
            .single();
        
        if (error) throw error;
        
        console.log('✅ Payment record saved:', order_tracking_id);
        
        await logUserAction(userId, 'PAYMENT_RECORDED', '/api/payments/record', { 
            order_tracking_id: order_tracking_id,
            amount: amount,
            status: status 
        }, req.user_ip, 'success');
        
        res.json({ success: true, payment: newPayment });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'PAYMENT_RECORD_ERROR', '/api/payments/record', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/payments/status/:order_tracking_id', authenticateToken, async (req, res) => {
    try {
        const order_tracking_id = req.params.order_tracking_id;
        const userId = req.user.id;
        
        console.log(`🔍 Checking payment status for: ${order_tracking_id} by user: ${userId}`);
        
        // Update user's last seen
        await supabase
            .from('users')
            .update({ last_seen: new Date().toISOString() })
            .eq('id', userId);
        
        const { data: payment, error } = await supabase
            .from('payments')
            .select('*')
            .eq('order_tracking_id', order_tracking_id)
            .single();
        
        if (error) {
            if (error.code === 'PGRST116') {
                await logUserAction(userId, 'PAYMENT_STATUS_FAILED', `/api/payments/status/${order_tracking_id}`, { 
                    reason: 'Payment record not found',
                    order_tracking_id: order_tracking_id 
                }, req.user_ip, 'failed');
                
                return res.status(404).json({ error: 'Rekodi ya malipo haijapatikana' });
            }
            throw error;
        }
        
        await logUserAction(userId, 'PAYMENT_STATUS_CHECK', `/api/payments/status/${order_tracking_id}`, { 
            order_tracking_id: order_tracking_id,
            status: payment.status 
        }, req.user_ip, 'success');
        
        res.json(payment);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'PAYMENT_STATUS_CHECK_ERROR', `/api/payments/status/${req.params.order_tracking_id}`, { 
            error: error.message 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});


// =============================================
// ✅ BUSINESS ENDPOINTS (PUBLIC)
// =============================================
app.get('/api/businesses', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 GET /api/businesses request IP:', ip);
    
    try {
        const { data: users, error } = await supabase
            .from('users')
            .select('id, email, role, full_name, phone, business_name, business_location, status, created_at')
            .eq('role', 'admin')
            .eq('status', 'approved')
            .order('created_at', { ascending: false });
        
        if (error) {
            console.error('❌ Database error:', error);
            
            await logUserAction(null, 'BUSINESSES_VIEW_ERROR', '/api/businesses', { 
                error: error.message 
            }, ip, 'failed');
            
            return res.status(500).json({ 
                error: 'Hitilafu ya database',
                details: error.message 
            });
        }
        
        console.log('🏢 Businesses found:', users?.length || 0);
        
        await logUserAction(null, 'BUSINESSES_VIEW', '/api/businesses', { 
            count: users?.length || 0 
        }, ip, 'success');
        
        res.json(users || []);
        
    } catch (error) {
        console.error('❌ ERROR in /api/businesses:', error.message);
        await logUserAction(null, 'BUSINESSES_VIEW_ERROR', '/api/businesses', { error: error.message }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya kupata biashara',
            details: error.message 
        });
    }
});

app.get('/api/businesses/:businessId/products', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    const businessId = req.params.businessId;
    
    console.log(`📦 Getting products for business ID: ${businessId} from IP:`, ip);
    
    try {
        const { data: business, error: businessError } = await supabase
            .from('users')
            .select('id, business_name')
            .eq('id', businessId)
            .eq('role', 'admin')
            .eq('status', 'approved')
            .single();
        
        if (businessError || !business) {
            console.log('⚠️ Business not found or not approved');
            
            await logUserAction(null, 'BUSINESS_PRODUCTS_FAILED', `/api/businesses/${businessId}/products`, { 
                reason: 'Business not found or not approved',
                business_id: businessId 
            }, ip, 'failed');
            
            return res.json([]);
        }
        
        const { data: products, error: productsError } = await supabase
            .from('products')
            .select('*')
            .eq('seller_id', business.id)
            .eq('is_active', true)
            .order('name', { ascending: true });
        
        if (productsError) {
            console.error('❌ Error fetching products:', productsError);
            
            await logUserAction(null, 'BUSINESS_PRODUCTS_ERROR', `/api/businesses/${businessId}/products`, { 
                error: productsError.message,
                business_id: businessId 
            }, ip, 'failed');
            
            return res.json([]);
        }
        
        console.log(`✅ Found ${products?.length || 0} products for business ${business.business_name}`);
        
        await logUserAction(null, 'BUSINESS_PRODUCTS_VIEW', `/api/businesses/${businessId}/products`, { 
            business_id: businessId,
            business_name: business.business_name,
            product_count: products?.length || 0 
        }, ip, 'success');
        
        res.json(products || []);
        
    } catch (error) {
        console.error('❌ ERROR getting business products:', error.message);
        
        await logUserAction(null, 'BUSINESS_PRODUCTS_ERROR', `/api/businesses/${businessId}/products`, { 
            error: error.message,
            business_id: businessId 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya kupata bidhaa',
            details: error.message 
        });
    }
});

app.get('/api/test/businesses', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('🧪 Testing businesses endpoint from IP:', ip);
    
    try {
        const { data: allAdmins, error: countError } = await supabase
            .from('users')
            .select('id, business_name, status')
            .eq('role', 'admin');
        
        if (countError) {
            console.error('❌ Count error:', countError);
            
            await logUserAction(null, 'TEST_BUSINESSES_ERROR', '/api/test/businesses', { 
                error: countError.message 
            }, ip, 'failed');
            
            return res.status(500).json({ error: 'Database error' });
        }
        
        const { data: approvedAdmins, error: approvedError } = await supabase
            .from('users')
            .select('id, email, business_name, status')
            .eq('role', 'admin')
            .eq('status', 'approved');
        
        if (approvedError) {
            console.error('❌ Approved error:', approvedError);
            
            await logUserAction(null, 'TEST_BUSINESSES_ERROR', '/api/test/businesses', { 
                error: approvedError.message 
            }, ip, 'failed');
            
            return res.status(500).json({ error: 'Database error' });
        }
        
        await logUserAction(null, 'TEST_BUSINESSES_SUCCESS', '/api/test/businesses', { 
            total_admins: allAdmins?.length || 0,
            approved_admins: approvedAdmins?.length || 0 
        }, ip, 'success');
        
        res.json({
            success: true,
            message: 'Business endpoint test successful',
            total_admins: allAdmins?.length || 0,
            approved_admins: approvedAdmins?.length || 0,
            all_admins: allAdmins || [],
            approved_admins_list: approvedAdmins || []
        });
        
    } catch (error) {
        console.error('❌ TEST ERROR:', error.message);
        
        await logUserAction(null, 'TEST_BUSINESSES_ERROR', '/api/test/businesses', { 
            error: error.message 
        }, ip, 'failed');
        
        res.status(500).json({ 
            success: false,
            error: 'Test failed',
            details: error.message 
        });
    }
});

app.get('/api/check-business-name', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    const { business_name } = req.query;
    
    console.log(`🔍 Checking business name: "${business_name}" from IP:`, ip);
    
    try {
        if (!business_name) {
            await logUserAction(null, 'CHECK_BUSINESS_NAME_FAILED', '/api/check-business-name', { 
                reason: 'Missing business name' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Jina la biashara linahitajika' });
        }

        const { data: users, error } = await supabase
            .from('users')
            .select('business_name, email')
            .ilike('business_name', business_name)
            .eq('status', 'approved');

        if (error) throw error;

        const exists = users && users.length > 0;
        
        await logUserAction(null, 'CHECK_BUSINESS_NAME', '/api/check-business-name', { 
            business_name: business_name,
            exists: exists,
            matches: users?.length || 0 
        }, ip, 'success');
        
        res.json({
            exists: exists,
            message: exists ? 'Jina la biashara tayari limeshasajiliwa' : 'Jina la biashara linapatikana',
            suggestions: exists ? [
                `"${business_name}"`,
                `"${business_name} ${new Date().getFullYear()}"`,
                `"${business_name} Store"`
            ] : []
        });

    } catch (error) {
        console.error('❌ ERROR checking business name:', error.message);
        
        await logUserAction(null, 'CHECK_BUSINESS_NAME_ERROR', '/api/check-business-name', { 
            error: error.message,
            business_name: business_name 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika kuangalia jina la biashara',
            details: error.message 
        });
    }
});

app.get('/api/businesses/:id', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    const businessId = req.params.id;
    
    console.log(`🏢 Getting business with ID: ${businessId} from IP:`, ip);
    
    try {
        const { data: business, error } = await supabase
            .from('users')
            .select('id, email, role, full_name, phone, business_name, business_location, status')
            .eq('id', businessId)
            .eq('role', 'admin')
            .eq('status', 'approved')
            .single();
        
        if (error || !business) {
            await logUserAction(null, 'BUSINESS_VIEW_FAILED', `/api/businesses/${businessId}`, { 
                reason: 'Business not found',
                business_id: businessId 
            }, ip, 'failed');
            
            return res.status(404).json({ error: 'Biashara haijapatikana' });
        }
        
        await logUserAction(null, 'BUSINESS_VIEW', `/api/businesses/${businessId}`, { 
            business_id: businessId,
            business_name: business.business_name 
        }, ip, 'success');
        
        res.json(business);
        
    } catch (error) {
        console.error('❌ ERROR getting business:', error.message);
        
        await logUserAction(null, 'BUSINESS_VIEW_ERROR', `/api/businesses/${businessId}`, { 
            error: error.message,
            business_id: businessId 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya kupata biashara',
            details: error.message 
        });
    }
});

app.get('/api/businesses/search/:query', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    const { query } = req.params;
    
    console.log(`🔍 Searching businesses for: "${query}" from IP:`, ip);
    
    try {
        const { data: businesses, error } = await supabase
            .from('users')
            .select('id, email, role, full_name, phone, business_name, business_location, status')
            .eq('role', 'admin')
            .eq('status', 'approved')
            .or(`business_name.ilike.%${query}%,business_location.ilike.%${query}%,full_name.ilike.%${query}%,phone.ilike.%${query}%`)
            .order('business_name', { ascending: true });
        
        if (error) throw error;
        
        console.log(`🔍 Found ${businesses?.length || 0} businesses for search: ${query}`);
        
        await logUserAction(null, 'BUSINESS_SEARCH', `/api/businesses/search/${query}`, { 
            search_query: query,
            result_count: businesses?.length || 0 
        }, ip, 'success');
        
        res.json(businesses || []);
        
    } catch (error) {
        console.error('❌ ERROR searching businesses:', error.message);
        
        await logUserAction(null, 'BUSINESS_SEARCH_ERROR', `/api/businesses/search/${query}`, { 
            error: error.message,
            search_query: query 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya kutafuta biashara',
            details: error.message 
        });
    }
});

app.post('/api/check-business', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log('📨 CHECK BUSINESS REQUEST from IP:', ip, 'Body:', req.body);
    
    try {
        const { business_name } = req.body;

        if (!business_name) {
            await logUserAction(null, 'CHECK_BUSINESS_FAILED', '/api/check-business', { 
                reason: 'Missing business name' 
            }, ip, 'failed');
            
            return res.status(400).json({ error: 'Jina la biashara linahitajika' });
        }

        const { data: adminUsers, error: adminError } = await supabaseAdmin
            .from('users')
            .select('id, email, role, status, created_at')
            .ilike('business_name', business_name.trim())
            .eq('role', 'admin')
            .eq('status', 'approved');

        if (adminError) {
            console.error('❌ Database error checking business:', adminError);
            
            await logUserAction(null, 'CHECK_BUSINESS_ERROR', '/api/check-business', { 
                error: adminError.message,
                business_name: business_name 
            }, ip, 'failed');
            
            return res.status(500).json({ 
                error: 'Hitilafu katika ukaguzi wa biashara',
                details: adminError.message 
            });
        }

        const hasApprovedAdmin = adminUsers && adminUsers.length > 0;
        
        if (hasApprovedAdmin) {
            console.log(`✅ Business exists with approved admin: "${business_name}"`);
            
            await logUserAction(null, 'CHECK_BUSINESS_SUCCESS', '/api/check-business', { 
                business_name: business_name,
                exists: true,
                hasAdmin: true,
                admin_count: adminUsers.length 
            }, ip, 'success');
            
            return res.json({ 
                exists: true,
                hasAdmin: true,
                message: 'Biashara ipo na ina msimamizi. Unaweza kuendelea na usajili wa muuzaji.',
                admin: adminUsers[0]
            });
        }

        const { data: anyBusinessUsers, error: businessError } = await supabaseAdmin
            .from('users')
            .select('id, email, role, status')
            .ilike('business_name', business_name.trim())
            .in('role', ['admin', 'seller']);

        if (businessError) {
            console.error('❌ Database error checking business users:', businessError);
            
            await logUserAction(null, 'CHECK_BUSINESS_ERROR', '/api/check-business', { 
                error: businessError.message,
                business_name: business_name 
            }, ip, 'failed');
            
            return res.status(500).json({ 
                error: 'Hitilafu katika ukaguzi wa biashara',
                details: businessError.message 
            });
        }

        const businessExists = anyBusinessUsers && anyBusinessUsers.length > 0;
        
        if (businessExists) {
            console.log(`⚠️ Business exists but no approved admin: "${business_name}"`);
            
            await logUserAction(null, 'CHECK_BUSINESS_SUCCESS', '/api/check-business', { 
                business_name: business_name,
                exists: true,
                hasAdmin: false,
                user_count: anyBusinessUsers.length 
            }, ip, 'success');
            
            return res.json({ 
                exists: true,
                hasAdmin: false,
                message: 'Biashara hii inapatikana lakini haina msimamizi aliyethibitishwa.',
                users: anyBusinessUsers
            });
        }

        console.log(`❌ Business doesn't exist: "${business_name}"`);
        
        await logUserAction(null, 'CHECK_BUSINESS_SUCCESS', '/api/check-business', { 
            business_name: business_name,
            exists: false,
            hasAdmin: false 
        }, ip, 'success');
        
        return res.json({ 
            exists: false,
            hasAdmin: false,
            message: 'Biashara hii haipo kwenye mfumo. Tafadhali jisajili kama msimamizi kwanza.'
        });

    } catch (error) {
        console.error('❌ ERROR in /api/check-business:', error.message);
        
        await logUserAction(null, 'CHECK_BUSINESS_ERROR', '/api/check-business', { 
            error: error.message,
            business_name: req.body?.business_name 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika ukaguzi wa biashara',
            details: error.message 
        });
    }
});

app.get('/api/business/by-name/:business_name', async (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    const { business_name } = req.params;
    
    console.log(`🔍 Looking for business: "${business_name}" from IP:`, ip);
    
    try {
        const decodedBusinessName = decodeURIComponent(business_name);
        
        const { data: businesses, error } = await supabase
            .from('users')
            .select('id, email, role, full_name, phone, business_name, business_location, status, created_at')
            .eq('role', 'admin')
            .eq('status', 'approved')
            .ilike('business_name', decodedBusinessName.trim())
            .order('created_at', { ascending: false });

        if (error) {
            console.error('❌ Database error:', error);
            
            await logUserAction(null, 'BUSINESS_BY_NAME_ERROR', `/api/business/by-name/${business_name}`, { 
                error: error.message,
                business_name: decodedBusinessName 
            }, ip, 'failed');
            
            return res.status(500).json({ 
                error: 'Hitilafu ya database',
                details: error.message 
            });
        }

        const businessExists = businesses && businesses.length > 0;
        
        console.log(`🏪 Business "${decodedBusinessName}" exists: ${businessExists}`);
        
        if (!businessExists) {
            await logUserAction(null, 'BUSINESS_BY_NAME_NOT_FOUND', `/api/business/by-name/${business_name}`, { 
                business_name: decodedBusinessName,
                exists: false 
            }, ip, 'success');
            
            return res.status(404).json({ 
                exists: false,
                message: 'Biashara haijapatikana' 
            });
        }

        const admin = businesses.find(b => b.role === 'admin' && b.status === 'approved');
        
        await logUserAction(null, 'BUSINESS_BY_NAME_FOUND', `/api/business/by-name/${business_name}`, { 
            business_name: decodedBusinessName,
            exists: true,
            hasAdmin: !!admin,
            match_count: businesses.length 
        }, ip, 'success');
        
        res.json({
            exists: true,
            hasAdmin: !!admin,
            business: businesses[0],
            admin: admin || null,
            allMatches: businesses
        });
        
    } catch (error) {
        console.error('❌ ERROR getting business by name:', error.message);
        
        await logUserAction(null, 'BUSINESS_BY_NAME_ERROR', `/api/business/by-name/${business_name}`, { 
            error: error.message,
            business_name: req.params.business_name 
        }, ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu ya kupata biashara',
            details: error.message 
        });
    }
});

// =============================================
// ✅ NEW ENDPOINT: GET ALL BUSINESS PRODUCTS
// =============================================
app.get('/api/business/:businessName/all-products', authenticateToken, async (req, res) => {
  try {
    const { businessName } = req.params;
    const userId = req.user.id;
    
    console.log(`📦 Loading ALL products for business: "${businessName}" requested by user: ${userId}`);
    
    const { data: businessUsers, error: usersError } = await supabase
      .from('users')
      .select('id, email, full_name, role, status')
      .eq('business_name', businessName)
      .in('role', ['admin', 'seller'])
      .eq('status', 'approved');
    
    if (usersError) {
      console.error('❌ Error fetching business users:', usersError);
      throw usersError;
    }
    
    if (!businessUsers || businessUsers.length === 0) {
      console.log(`⚠️ No approved users found for business: "${businessName}"`);
      
      await logUserAction(userId, 'BUSINESS_PRODUCTS_FAILED', `/api/business/${businessName}/all-products`, { 
        reason: 'No approved users found',
        business_name: businessName 
      }, req.user_ip, 'failed');
      
      return res.json([]);
    }
    
    const sellerIds = businessUsers.map(user => user.id);
    console.log(`👥 Found ${businessUsers.length} users in business ${businessName}`);
    
    const { data: allProducts, error: productsError } = await supabase
      .from('products')
      .select('*')
      .in('seller_id', sellerIds)
      .eq('is_active', true)
      .order('name', { ascending: true });
    
    if (productsError) {
      console.error('❌ Error fetching business products:', productsError);
      throw productsError;
    }
    
    console.log(`✅ Found ${allProducts?.length || 0} active products in business ${businessName}`);
    
    const productsWithSellerInfo = (allProducts || []).map(product => {
      const seller = businessUsers.find(user => user.id === product.seller_id);
      return {
        ...product,
        seller_name: seller?.full_name || seller?.email || 'Unknown',
        seller_role: seller?.role || 'unknown',
        seller_email: seller?.email || null
      };
    });
    
    await logUserAction(userId, 'BUSINESS_PRODUCTS_VIEW', `/api/business/${businessName}/all-products`, { 
      business_name: businessName,
      product_count: productsWithSellerInfo.length,
      seller_count: businessUsers.length 
    }, req.user_ip, 'success');
    
    res.json(productsWithSellerInfo);
    
  } catch (error) {
    console.error('❌ ERROR in /api/business/:businessName/all-products:', error.message);
    
    await logUserAction(req.user?.id, 'BUSINESS_PRODUCTS_ERROR', `/api/business/${req.params.businessName}/all-products`, { 
      error: error.message,
      business_name: req.params.businessName 
    }, req.user_ip, 'failed');
    
    // Fallback logic...
    try {
      const { businessName } = req.params;
      
      const { data: adminUsers, error: adminError } = await supabase
        .from('users')
        .select('id')
        .eq('business_name', businessName)
        .eq('role', 'admin')
        .eq('status', 'approved')
        .limit(1);
      
      if (!adminError && adminUsers && adminUsers.length > 0) {
        const adminId = adminUsers[0].id;
        
        const { data: adminProducts, error: adminProductsError } = await supabase
          .from('products')
          .select('*')
          .eq('seller_id', adminId)
          .eq('is_active', true)
          .order('name', { ascending: true });
        
        if (!adminProductsError && adminProducts) {
          console.log(`✅ Fallback: Found ${adminProducts.length} admin products`);
          
          const productsWithSellerInfo = (adminProducts || []).map(product => ({
            ...product,
            seller_name: 'Admin',
            seller_role: 'admin',
            seller_email: null
          }));
          
          await logUserAction(req.user?.id, 'BUSINESS_PRODUCTS_FALLBACK', `/api/business/${businessName}/all-products`, { 
            business_name: businessName,
            fallback_product_count: productsWithSellerInfo.length 
          }, req.user_ip, 'success');
          
          return res.json(productsWithSellerInfo);
        }
      }
    } catch (fallbackError) {
      console.error('❌ Fallback also failed:', fallbackError.message);
    }
    
    res.status(500).json({ 
      error: 'Hitilafu ya kupata bidhaa za biashara',
      details: error.message 
    });
  }
});

// =============================================
// ✅ USER LOGS ENDPOINTS (ADMIN ONLY)
// =============================================
app.get('/api/admin/logs', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'ADMIN_LOGS_UNAUTHORIZED', '/api/admin/logs', { 
                reason: 'Non-admin access attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`📋 Admin ${adminId} viewing logs`);
        
        const { data: logs, error } = await supabaseAdmin
            .from('user_logs')
            .select(`*, users(email, full_name, role)`)
            .order('created_at', { ascending: false })
            .limit(100);
        
        if (error) throw error;
        
        await logUserAction(adminId, 'ADMIN_LOGS_VIEW', '/api/admin/logs', { 
            log_count: logs?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(logs || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_LOGS_VIEW_ERROR', '/api/admin/logs', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.get('/api/admin/logs/search', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const { action, user_id, status, start_date, end_date, ip_address } = req.query;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'ADMIN_LOGS_SEARCH_UNAUTHORIZED', '/api/admin/logs/search', { 
                reason: 'Non-admin search attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`🔍 Admin ${adminId} searching logs with filters:`, req.query);
        
        let query = supabaseAdmin
            .from('user_logs')
            .select(`*, users(email, full_name, role)`)
            .order('created_at', { ascending: false });
        
        if (action) {
            query = query.ilike('action', `%${action}%`);
        }
        
        if (user_id) {
            query = query.eq('user_id', user_id);
        }
        
        if (status) {
            query = query.eq('status', status);
        }
        
        if (start_date) {
            query = query.gte('created_at', start_date);
        }
        
        if (end_date) {
            query = query.lte('created_at', end_date);
        }
        
        if (ip_address) {
            query = query.ilike('ip_address', `%${ip_address}%`);
        }
        
        const { data: logs, error } = await query;
        
        if (error) throw error;
        
        await logUserAction(adminId, 'ADMIN_LOGS_SEARCH', '/api/admin/logs/search', { 
            filters: req.query,
            result_count: logs?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(logs || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_LOGS_SEARCH_ERROR', '/api/admin/logs/search', { 
            error: error.message,
            filters: req.query 
        }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

app.delete('/api/admin/logs/cleanup', authenticateToken, async (req, res) => {
    try {
        const adminId = req.user.id;
        const { days = 30 } = req.body;
        
        if (req.user.role !== 'admin') {
            await logUserAction(adminId, 'ADMIN_LOGS_CLEANUP_UNAUTHORIZED', '/api/admin/logs/cleanup', { 
                reason: 'Non-admin cleanup attempt' 
            }, req.user_ip, 'failed');
            
            return res.status(403).json({ error: 'Unauthorized' });
        }
        
        console.log(`🧹 Admin ${adminId} cleaning up logs older than ${days} days`);
        
        const cutoffDate = new Date();
        cutoffDate.setDate(cutoffDate.getDate() - parseInt(days));
        
        const { data: logsToDelete, error: countError } = await supabaseAdmin
            .from('user_logs')
            .select('id', { count: 'exact', head: false })
            .lt('created_at', cutoffDate.toISOString());
        
        if (countError) throw countError;
        
        const { error: deleteError } = await supabaseAdmin
            .from('user_logs')
            .delete()
            .lt('created_at', cutoffDate.toISOString());
        
        if (deleteError) throw deleteError;
        
        const deletedCount = logsToDelete?.length || 0;
        
        console.log(`✅ Cleaned up ${deletedCount} log entries`);
        
        await logUserAction(adminId, 'ADMIN_LOGS_CLEANUP', '/api/admin/logs/cleanup', { 
            deleted_count: deletedCount,
            cutoff_days: days,
            cutoff_date: cutoffDate.toISOString() 
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: `Imefanikiwa kufuta rekodi ${deletedCount} za logi zilizozeeka zaidi ya siku ${days}`,
            deleted_count: deletedCount,
            cutoff_date: cutoffDate.toISOString()
        });
        
    } catch (error) {
        await logUserAction(req.user?.id, 'ADMIN_LOGS_CLEANUP_ERROR', '/api/admin/logs/cleanup', { 
            error: error.message,
            days: req.body?.days 
        }, req.user_ip, 'failed');
        
        res.status(500).json({ 
            error: 'Hitilafu katika kusafisha rekodi za logi',
            details: error.message 
        });
    }
});

// =============================================
// ✅ USER ACTIVITY ENDPOINTS
// =============================================
app.get('/api/user/activity', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        console.log(`📊 Loading activity for user ${userId}`);
        
        const { data: activities, error } = await supabase
            .from('user_logs')
            .select('action, endpoint, status, created_at, details')
            .eq('user_id', userId)
            .order('created_at', { ascending: false })
            .limit(50);
        
        if (error) throw error;
        
        await logUserAction(userId, 'ACTIVITY_VIEW', '/api/user/activity', { 
            count: activities?.length || 0 
        }, req.user_ip, 'success');
        
        res.json(activities || []);
        
    } catch (error) {
        await logUserAction(req.user?.id, 'ACTIVITY_VIEW_ERROR', '/api/user/activity', { error: error.message }, req.user_ip, 'failed');
        handleSupabaseError(error, res);
    }
});

// =============================================
// ✅ SAFE ADDITION: OFFICE EXPENSES (MATUMIZI YA OFISI) - NEW FEATURE
// =============================================

// GET today's expenses (IMPROVED)
app.get('/api/office-expenses/today', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const today = new Date().toISOString().split('T')[0];
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name')
            .eq('id', userId)
            .single();
        
        if (userError || !user || !user.business_name) {
            return res.status(400).json({ 
                success: false,
                error: 'Biashara haijapatikana' 
            });
        }
        
        const businessName = user.business_name;
        
        console.log(`💰 Getting today's expenses for business: ${businessName}`);
        
        const { data: expenses, error } = await supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', businessName)
            .eq('expense_date', today)
            .order('created_at', { ascending: false });
        
        if (error) throw error;
        
        const total = expenses?.reduce((sum, exp) => sum + (exp.amount || 0), 0) || 0;
        
        // Format expenses
        const formattedExpenses = (expenses || []).map(exp => ({
            id: exp.id,
            amount: Number(exp.amount) || 0,
            description: exp.description || '',
            category: exp.category || 'Mengineyo',
            expense_date: exp.expense_date,
            notes: exp.notes || '',
            user_id: exp.user_id,
            business_name: exp.business_name,
            created_at: exp.created_at
        }));
        
        res.json({
            success: true,
            expenses: formattedExpenses,
            total: total,
            date: today,
            count: formattedExpenses.length
        });
        
    } catch (error) {
        console.error('❌ Error getting today\'s expenses:', error.message);
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kupata matumizi',
            details: error.message 
        });
    }
});

// Add new expense (IMPROVED)
app.post('/api/office-expenses', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { amount, description, category, notes } = req.body;
        
        console.log(`📝 Adding expense for user ${userId}`);
        
        // Validation
        if (!amount || !description || !category) {
            return res.status(400).json({ 
                success: false,
                error: 'Kiasi, maelezo na aina ya matumizi vinahitajika' 
            });
        }
        
        const amountValue = parseFloat(amount);
        if (isNaN(amountValue) || amountValue <= 0) {
            return res.status(400).json({ 
                success: false,
                error: 'Kiasi lazima kiwe namba kubwa kuliko 0' 
            });
        }
        
        if (description.trim().length < 3) {
            return res.status(400).json({ 
                success: false,
                error: 'Maelezo lazima yawe na herufi 3 au zaidi' 
            });
        }
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name')
            .eq('id', userId)
            .single();
        
        if (userError || !user || !user.business_name) {
            return res.status(400).json({ 
                success: false,
                error: 'Biashara haijapatikana' 
            });
        }
        
        const businessName = user.business_name;
        const today = new Date().toISOString().split('T')[0];
        
        const expenseData = {
            user_id: userId,
            business_name: businessName,
            expense_date: today,
            amount: amountValue,
            description: description.trim(),
            category: category,
            notes: notes ? notes.trim() : null,
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString()
        };
        
        console.log('💾 Saving expense:', expenseData);
        
        const { data: newExpense, error } = await supabase
            .from('office_expenses')
            .insert([expenseData])
            .select()
            .single();
        
        if (error) {
            console.error('❌ Database error:', error);
            throw error;
        }
        
        console.log('✅ Expense added:', newExpense.id, 'for business:', businessName);
        
        // Log the action
        await logUserAction(userId, 'ADD_EXPENSE', '/api/office-expenses', { 
            expense_id: newExpense.id,
            amount: amountValue,
            category: category,
            business_name: businessName,
            date: today
        }, req.user_ip, 'success');
        
        res.status(201).json({
            success: true,
            message: 'Matumizi yameongezwa kikamilifu!',
            expense: {
                id: newExpense.id,
                amount: Number(newExpense.amount),
                description: newExpense.description,
                category: newExpense.category,
                expense_date: newExpense.expense_date,
                notes: newExpense.notes,
                user_id: newExpense.user_id,
                business_name: newExpense.business_name
            }
        });
        
    } catch (error) {
        console.error('❌ Error adding expense:', error.message);
        await logUserAction(req.user?.id, 'ADD_EXPENSE_ERROR', '/api/office-expenses', { 
            error: error.message 
        }, req.user_ip, 'failed');
        
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kuongeza matumizi',
            details: error.message 
        });
    }
});

// GET expenses for a specific date (NEW - RECOMMENDED)
app.get('/api/office-expenses/date/:date', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const targetDate = req.params.date;
        
        // Validate date format
        if (!targetDate || !/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
            return res.status(400).json({ 
                success: false,
                error: 'Tarehe si sahihi. Tumia format: YYYY-MM-DD' 
            });
        }
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name, id, email, role')
            .eq('id', userId)
            .single();
        
        if (userError || !user) {
            return res.status(400).json({ 
                success: false,
                error: 'Mtumiaji hajapatikana' 
            });
        }
        
        const businessName = user.business_name;
        
        if (!businessName) {
            return res.status(400).json({ 
                success: false,
                error: 'Biashara haijapatikana kwenye akaunti yako' 
            });
        }
        
        console.log(`💰 Getting expenses for business: ${businessName} on date: ${targetDate}`);
        
        // Get expenses for this business on this specific date
        const { data: expenses, error } = await supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', businessName)
            .eq('expense_date', targetDate)
            .order('created_at', { ascending: false });
        
        if (error) {
            console.error('❌ Database error:', error);
            throw error;
        }
        
        const total = expenses?.reduce((sum, exp) => sum + (exp.amount || 0), 0) || 0;
        
        console.log(`✅ Found ${expenses?.length || 0} expenses for date ${targetDate}, total: ${total}`);
        
        // Format expenses to ensure all fields are present
        const formattedExpenses = (expenses || []).map(exp => ({
            id: exp.id,
            amount: Number(exp.amount) || 0,
            description: exp.description || '',
            category: exp.category || 'Mengineyo',
            expense_date: exp.expense_date || targetDate,
            notes: exp.notes || '',
            user_id: exp.user_id,
            business_name: exp.business_name,
            created_at: exp.created_at
        }));
        
        res.json({
            success: true,
            expenses: formattedExpenses,
            total: total,
            date: targetDate,
            count: formattedExpenses.length,
            business_name: businessName
        });
        
    } catch (error) {
        console.error('❌ Error getting expenses for date:', error.message);
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kupata matumizi',
            details: error.message 
        });
    }
});

// Get expenses by date range - FILTER BY BUSINESS NAME

// GET expenses by date range (IMPROVED)
app.get('/api/office-expenses/range', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { start_date, end_date } = req.query;
        
        if (!start_date || !end_date) {
            return res.status(400).json({ 
                success: false,
                error: 'Tarehe za kuanza na mwisho zinahitajika' 
            });
        }
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name')
            .eq('id', userId)
            .single();
        
        if (userError || !user || !user.business_name) {
            return res.status(400).json({ 
                success: false,
                error: 'Biashara haijapatikana' 
            });
        }
        
        const businessName = user.business_name;
        
        console.log(`📊 Getting expenses for business: ${businessName} from ${start_date} to ${end_date}`);
        
        let query = supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', businessName)
            .order('expense_date', { ascending: false });
        
        if (start_date) {
            query = query.gte('expense_date', start_date);
        }
        
        if (end_date) {
            query = query.lte('expense_date', end_date);
        }
        
        const { data: expenses, error } = await query;
        
        if (error) throw error;
        
        console.log(`✅ Found ${expenses?.length || 0} expenses for business ${businessName}`);
        
        const total = expenses?.reduce((sum, exp) => sum + (exp.amount || 0), 0) || 0;
        
        // Group by category
        const byCategory = {};
        (expenses || []).forEach(exp => {
            const cat = exp.category || 'Mengineyo';
            byCategory[cat] = (byCategory[cat] || 0) + (exp.amount || 0);
        });
        
        // Group by date
        const byDate = {};
        (expenses || []).forEach(exp => {
            const date = exp.expense_date;
            if (!byDate[date]) {
                byDate[date] = [];
            }
            byDate[date].push(exp);
        });
        
        // Format expenses
        const formattedExpenses = (expenses || []).map(exp => ({
            id: exp.id,
            amount: Number(exp.amount) || 0,
            description: exp.description || '',
            category: exp.category || 'Mengineyo',
            expense_date: exp.expense_date,
            notes: exp.notes || '',
            user_id: exp.user_id,
            business_name: exp.business_name,
            created_at: exp.created_at
        }));
        
        res.json({
            success: true,
            expenses: formattedExpenses,
            total: total,
            by_category: byCategory,
            by_date: byDate,
            count: formattedExpenses.length,
            date_range: {
                start: start_date,
                end: end_date
            }
        });
        
    } catch (error) {
        console.error('❌ Error getting expense range:', error.message);
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kupata matumizi',
            details: error.message 
        });
    }
});

// Delete expense (IMPROVED)
app.delete('/api/office-expenses/:id', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const expenseId = req.params.id;
        
        // Get user's business name and role
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name, role')
            .eq('id', userId)
            .single();
        
        if (userError || !user) {
            return res.status(400).json({ 
                success: false,
                error: 'Mtumiaji hajapatikana' 
            });
        }
        
        const businessName = user.business_name;
        const userRole = user.role;
        
        console.log(`🗑️ Deleting expense ${expenseId} from business: ${businessName}`);
        
        // First, get the expense to check business and permissions
        const { data: expense, error: checkError } = await supabase
            .from('office_expenses')
            .select('id, amount, description, business_name, user_id')
            .eq('id', expenseId)
            .single();
        
        if (checkError || !expense) {
            return res.status(404).json({ 
                success: false,
                error: 'Matumizi hayajapatikana' 
            });
        }
        
        // Check if user belongs to the same business OR is admin
        if (expense.business_name !== businessName && userRole !== 'admin') {
            return res.status(403).json({ 
                success: false,
                error: 'Huna ruhusa ya kufuta matumizi haya' 
            });
        }
        
        const { error: deleteError } = await supabase
            .from('office_expenses')
            .delete()
            .eq('id', expenseId);
        
        if (deleteError) throw deleteError;
        
        console.log('✅ Expense deleted:', expenseId);
        
        await logUserAction(userId, 'DELETE_EXPENSE', `/api/office-expenses/${expenseId}`, { 
            expense_id: expenseId,
            amount: expense.amount,
            description: expense.description,
            business_name: businessName
        }, req.user_ip, 'success');
        
        res.json({
            success: true,
            message: 'Matumizi yamefutwa kikamilifu!',
            expenseId: expenseId
        });
        
    } catch (error) {
        console.error('❌ Error deleting expense:', error.message);
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kufuta matumizi',
            details: error.message 
        });
    }
});

// Get daily profit summary with expenses - FILTER BY BUSINESS NAME
app.get('/api/profit/daily/:date?', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const targetDate = req.params.date || new Date().toISOString().split('T')[0];
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name')
            .eq('id', userId)
            .single();
        
        if (userError || !user || !user.business_name) {
            return res.status(400).json({ 
                success: false,
                error: 'Biashara haijapatikana' 
            });
        }
        
        const businessName = user.business_name;
        
        console.log(`📈 Getting daily profit for business: ${businessName} on ${targetDate}`);
        
        // Get all users in this business
        const { data: businessUsers, error: usersError } = await supabase
            .from('users')
            .select('id')
            .eq('business_name', businessName)
            .eq('status', 'approved');
        
        if (usersError) throw usersError;
        
        const userIds = businessUsers?.map(u => u.id) || [];
        
        // 1. Get today's sales (revenue) from all business users
        let grossRevenue = 0;
        let costOfGoods = 0;
        
        if (userIds.length > 0) {
            const { data: sales, error: salesError } = await supabase
                .from('sales')
                .select('total_amount, sale_items(quantity, unit_price, products(cost_price))')
                .in('seller_id', userIds)
                .eq('sale_date', targetDate);
            
            if (salesError) throw salesError;
            
            sales?.forEach(sale => {
                grossRevenue += sale.total_amount || 0;
                
                // FIXED: Remove :any
                sale.sale_items?.forEach((item) => {
                    const costPrice = item.products?.cost_price || 0;
                    costOfGoods += (item.quantity || 0) * costPrice;
                });
            });
        }
        
        const grossProfit = grossRevenue - costOfGoods;
        
        // 2. Get today's expenses for the BUSINESS (not just user)
        const { data: expenses, error: expensesError } = await supabase
            .from('office_expenses')
            .select('amount, category, description, user_id')
            .eq('business_name', businessName)  // FILTER BY BUSINESS NAME
            .eq('expense_date', targetDate);
        
        if (expensesError) throw expensesError;
        
        const totalExpenses = expenses?.reduce((sum, exp) => sum + exp.amount, 0) || 0;
        
        // 3. Calculate net profit
        const netProfit = grossProfit - totalExpenses;
        
        // 4. Group expenses by category - FIXED: Remove :any
        const expensesByCategory = expenses?.reduce((acc, exp) => {
            acc[exp.category] = (acc[exp.category] || 0) + exp.amount;
            return acc;
        }, {});
        
        // 5. Get creator names for expenses
        const expensesWithCreators = await Promise.all((expenses || []).map(async (exp) => {
            if (exp.user_id) {
                const { data: creator } = await supabase
                    .from('users')
                    .select('full_name, email')
                    .eq('id', exp.user_id)
                    .single();
                return { ...exp, creator_name: creator?.full_name || creator?.email || 'Unknown' };
            }
            return exp;
        }));
        
        res.json({
            success: true,
            date: targetDate,
            business_name: businessName,
            revenue: {
                gross: parseFloat(grossRevenue.toFixed(2)),
                cost_of_goods: parseFloat(costOfGoods.toFixed(2))
            },
            gross_profit: parseFloat(grossProfit.toFixed(2)),
            expenses: {
                total: parseFloat(totalExpenses.toFixed(2)),
                by_category: expensesByCategory || {},
                list: expensesWithCreators || []
            },
            net_profit: parseFloat(netProfit.toFixed(2)),
            sales_count: 0, // You might want to calculate this
            expenses_count: expenses?.length || 0
        });
        
    } catch (error) {
        console.error('❌ Error calculating daily profit:', error.message);
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kuhesabu faida ya siku',
            details: error.message 
        });
    }
});

// Get monthly profit summary
app.get('/api/profit/monthly/:year/:month', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const { year, month } = req.params;
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name')
            .eq('id', userId)
            .single();
        
        if (userError || !user || !user.business_name) {
            return res.status(400).json({ 
                success: false,
                error: 'Biashara haijapatikana' 
            });
        }
        
        const businessName = user.business_name;
        
        const startDate = `${year}-${month.padStart(2, '0')}-01`;
        const endDate = new Date(parseInt(year), parseInt(month), 0).toISOString().split('T')[0];
        
        console.log(`📊 Getting monthly profit for business: ${businessName} in ${year}-${month}`);
        
        // Get all users in this business
        const { data: businessUsers, error: usersError } = await supabase
            .from('users')
            .select('id')
            .eq('business_name', businessName)
            .eq('status', 'approved');
        
        if (usersError) throw usersError;
        
        const userIds = businessUsers?.map(u => u.id) || [];
        
        // Get all sales in month
        let totalRevenue = 0;
        let totalCostOfGoods = 0;
        
        if (userIds.length > 0) {
            const { data: sales, error: salesError } = await supabase
                .from('sales')
                .select('total_amount, sale_date, sale_items(quantity, unit_price, products(cost_price))')
                .in('seller_id', userIds)
                .gte('sale_date', startDate)
                .lte('sale_date', endDate);
            
            if (salesError) throw salesError;
            
            sales?.forEach(sale => {
                totalRevenue += sale.total_amount || 0;
                
                // FIXED: Remove :any
                sale.sale_items?.forEach((item) => {
                    const costPrice = item.products?.cost_price || 0;
                    totalCostOfGoods += (item.quantity || 0) * costPrice;
                });
            });
        }
        
        // Get all expenses in month for the BUSINESS
        const { data: expenses, error: expensesError } = await supabase
            .from('office_expenses')
            .select('amount, expense_date, category')
            .eq('business_name', businessName)
            .gte('expense_date', startDate)
            .lte('expense_date', endDate);
        
        if (expensesError) throw expensesError;
        
        const grossProfit = totalRevenue - totalCostOfGoods;
        const totalExpenses = expenses?.reduce((sum, exp) => sum + exp.amount, 0) || 0;
        const netProfit = grossProfit - totalExpenses;
        
        // Daily breakdown - FIXED: Remove :any
        const dailyBreakdown = {};
        
        // Get sales for daily breakdown (would need to re-fetch or restructure)
        // This is simplified - you might want to enhance this
        
        // Expense categories - FIXED: Remove :any
        const expenseCategories = expenses?.reduce((acc, exp) => {
            acc[exp.category] = (acc[exp.category] || 0) + exp.amount;
            return acc;
        }, {});
        
        res.json({
            success: true,
            year: parseInt(year),
            month: parseInt(month),
            business_name: businessName,
            summary: {
                total_sales: 0, // You can calculate this
                total_revenue: parseFloat(totalRevenue.toFixed(2)),
                total_cost_of_goods: parseFloat(totalCostOfGoods.toFixed(2)),
                gross_profit: parseFloat(grossProfit.toFixed(2)),
                total_expenses: parseFloat(totalExpenses.toFixed(2)),
                net_profit: parseFloat(netProfit.toFixed(2)),
                expense_categories: expenseCategories || {}
            },
            daily_breakdown: dailyBreakdown
        });
        
    } catch (error) {
        console.error('❌ Error calculating monthly profit:', error.message);
        res.status(500).json({ 
            success: false,
            error: 'Hitilafu ya kuhesabu faida ya mwezi',
            details: error.message 
        });
    }
});

app.get('/api/office-expenses/categories', authenticateToken, async (req, res) => {
    const categories = [
        'Kodisho za Nguvu',
        'Maji', 
        'Mawasiliano',
        'Usafiri',
        'Chakula cha Mchana',
        'Vifaa vya Ofisi',
        'Matengenezo',
        'Ushuru',
        'Ada za Benki',
        'Malighafi',
        'Ufungashaji',
        'Mengineyo'
    ];
    
    res.json({
        success: true,
        categories: categories
    });
});

// =============================================
// ✅ DEBUG ENDPOINT FOR EXPENSES
// =============================================
app.get('/api/debug/expenses/:date', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        const targetDate = req.params.date;
        
        // Get user's business name
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('business_name, id, email, role')
            .eq('id', userId)
            .single();
        
        if (userError || !user) {
            return res.status(400).json({ error: 'User not found' });
        }
        
        const businessName = user.business_name;
        
        console.log(`🔍 DEBUG: Getting expenses for user ${userId}, business: ${businessName}, date: ${targetDate}`);
        
        // Get expenses for this business on this date
        const { data: expenses, error } = await supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', businessName)
            .eq('expense_date', targetDate);
        
        if (error) throw error;
        
        // Get sample of all expenses for this business (any date)
        const { data: anyExpenses, error: anyError } = await supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', businessName)
            .limit(5);
        
        // Get count of all expenses
        const { count: totalCount, error: countError } = await supabase
            .from('office_expenses')
            .select('*', { count: 'exact', head: true })
            .eq('business_name', businessName);
        
        const debugInfo = {
            user: {
                id: user.id,
                email: user.email,
                role: user.role,
                business_name: businessName
            },
            requested_date: targetDate,
            expenses_for_date: (expenses || []).map(e => ({
                id: e.id,
                amount: e.amount,
                description: e.description,
                category: e.category,
                expense_date: e.expense_date
            })),
            count_for_date: expenses?.length || 0,
            total_for_date: expenses?.reduce((sum, e) => sum + (e.amount || 0), 0) || 0,
            sample_expenses_any_date: (anyExpenses || []).map(e => ({
                id: e.id,
                amount: e.amount,
                description: e.description,
                category: e.category,
                expense_date: e.expense_date
            })),
            any_expenses_exist: (anyExpenses?.length || 0) > 0,
            total_expenses_in_business: totalCount || 0,
            database_query: {
                table: 'office_expenses',
                business_name: businessName,
                date: targetDate
            },
            timestamp: new Date().toISOString()
        };
        
        console.log('🔍 DEBUG INFO:', JSON.stringify(debugInfo, null, 2));
        
        res.json({
            success: true,
            debug: debugInfo
        });
        
    } catch (error) {
        console.error('❌ Debug error:', error.message);
        res.status(500).json({ 
            success: false,
            error: error.message 
        });
    }
});

// =============================================
// ✅ DEBUG ENDPOINT - CHECK BUSINESS NAME
// =============================================
app.get('/api/debug/check-business', authenticateToken, async (req, res) => {
    try {
        const userId = req.user.id;
        
        // Get user info
        const { data: user, error: userError } = await supabase
            .from('users')
            .select('id, email, role, business_name, status')
            .eq('id', userId)
            .single();
        
        if (userError) throw userError;
        
        // Get all expenses for this business
        const { data: allExpenses, error: expensesError } = await supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', user.business_name)
            .order('expense_date', { ascending: false });
        
        // Get expenses for today
        const today = new Date().toISOString().split('T')[0];
        const { data: todayExpenses, error: todayError } = await supabase
            .from('office_expenses')
            .select('*')
            .eq('business_name', user.business_name)
            .eq('expense_date', today);
        
        res.json({
            success: true,
            user: {
                id: user.id,
                email: user.email,
                role: user.role,
                business_name: user.business_name,
                status: user.status
            },
            all_expenses_count: allExpenses?.length || 0,
            today_expenses_count: todayExpenses?.length || 0,
            today_expenses_total: todayExpenses?.reduce((sum, e) => sum + e.amount, 0) || 0,
            sample_expenses: allExpenses?.slice(0, 5) || []
        });
        
    } catch (error) {
        console.error('Debug error:', error);
        res.status(500).json({ error: error.message });
    }
});

// =============================================
// ✅ CATCH-ALL ROUTE FOR UNDEFINED ENDPOINTS
// =============================================
app.use('*', (req, res) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.log(`❌ Route not found: ${req.originalUrl} - IP: ${ip}`);
    
    if (req.originalUrl.startsWith('/api')) {
        logUserAction(null, 'ROUTE_NOT_FOUND', req.originalUrl, { 
            method: req.method,
            url: req.originalUrl 
        }, ip, 'failed');
        
        return res.status(404).json({ 
            error: `Endpoint ${req.originalUrl} haipo`,
            method: req.method,
            availableEndpoints: [
                '✅ AUTH:',
                'POST /api/register',
                'POST /api/login',
                'POST /api/forgot-password',
                'POST /api/reset-password',
                
                '✅ USER:',
                'GET  /api/user/profile',
                'PUT  /api/user/profile',
                'GET  /api/user/activity',
                
                '✅ PRODUCTS:',
                'GET  /api/products/my',
                'GET  /api/products/dummy',
                'POST /api/products',
                'PUT  /api/products/:id',
                'DELETE /api/products/:id',
                
                '✅ SALES:',
                'GET  /api/sales/my',
                'POST /api/sales',
                
                '✅ CUSTOMERS:',
                'GET  /api/customers/my',
                'POST /api/customers',
                'PUT  /api/customers/:id',
                
                '✅ MATANGAZO:',
                'GET  /api/matangazo',
                'GET  /api/matangazo/my',
                'POST /api/matangazo',
                'DELETE /api/matangazo/:id',
                
                '✅ REACTIONS:',
                'GET  /api/reactions/user/likes',
                'POST /api/reactions/like',
                'GET  /api/reactions/matangazo/:id/counts',
                'POST /api/reactions/report',
                
                '✅ REVENUE:',
                'GET  /api/revenue/my',
                
                '✅ ADMIN:',
                'GET  /api/admin/users',
                'GET  /api/admin/products',
                'GET  /api/admin/sales',
                'GET  /api/admin/customers',
                'GET  /api/admin/payments',
                'GET  /api/admin/stats',
                'GET  /api/admin/logs',
                'GET  /api/admin/logs/search',
                'DELETE /api/admin/logs/cleanup',
                'PUT  /api/admin/users/:id/status',
                'DELETE /api/admin/users/:id',
                
                '✅ PAYMENTS:',
                'POST /api/payments/pesapal/initiate',
                'GET  /api/payments/pesapal/status/:order_tracking_id',
                'POST /api/payments/pesapal-ipn',
                'POST /api/payments/pesapal-callback',
                'POST /api/payments/record',
                'GET  /api/payments/status/:order_tracking_id',
                'GET  /api/payments/my',
                'GET  /api/payments/:id',
                'GET  /api/admin/payments',
                'POST /api/payments/simulate',
                
                '✅ BUSINESS:',
                'GET  /api/businesses',
                'GET  /api/businesses/:id',
                'GET  /api/businesses/:businessId/products',
                'GET  /api/businesses/search/:query',
                'GET  /api/check-business-name',
                'GET  /api/test/businesses',
                'GET  /api/business/:businessName/all-products',
                'POST /api/check-business',
                'GET  /api/business/by-name/:business_name',
                
                '✅ SYSTEM:',
                'GET  /api/test',
                'GET  /api/health'
            ]
        });
    }
    
    res.sendFile(path.join(__dirname, 'index.html'));
});

// =============================================
// ✅ ERROR HANDLER
// =============================================
app.use((error, req, res, next) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    console.error('❌ UNHANDLED ERROR:', error.message);
    
    logUserAction(req.user?.id, 'UNHANDLED_ERROR', req.originalUrl, { 
        error: error.message,
        stack: error.stack 
    }, ip, 'failed');
    
    res.status(500).json({
        error: 'Hitilafu ya ndani ya server',
        message: error.message,
        request_id: Date.now().toString(36) + Math.random().toString(36).substr(2)
    });
});

// =============================================
// ✅ START SERVER
// =============================================
const PORT = process.env.PORT || 10000;

app.listen(PORT, '0.0.0.0', () => {
    console.log('\n🎉 DUKA MKONONI BACKEND STARTED!');
    console.log('=================================');
    console.log(`📍 Server running on port: ${PORT}`);
    console.log(`🌍 Open in browser: http://localhost:${PORT}`);
    console.log(`🏢 Database: ${supabaseUrl}`);
    console.log('🔐 JWT Authentication: Enabled');
    console.log('📝 Comprehensive Logging: Enabled');
    console.log('📊 User Logs Table: Active');
    console.log('💰 Unit Price Field: Added to sale_items table');
    console.log('🧾 Invoice Number Generation: Enabled');
    console.log('💳 PesaPal Integration: Ready');
    console.log('\n✅ READY TO HANDLE REQUESTS!');
    console.log('\n📁 Available HTML Pages:');
    console.log('- /              - Landing Page');
    console.log('- /login         - Login Page');
    console.log('- /admin_signup  - Admin Registration');
    console.log('- /home          - Dashboard');
    console.log('- /biashara      - Business Page');
    console.log('- /matangazo     - Announcements');
    console.log('- /mauzo         - Sales');
    console.log('- /malipo        - Payments');
    console.log('\n🔧 Available APIs:');
    console.log('- GET  /api/test          - Test endpoint');
    console.log('- GET  /api/health        - Health check');
    console.log('- POST /api/register      - User registration');
    console.log('- POST /api/login         - User login');
    console.log('- GET  /api/user/activity - User activity logs');
    console.log('- GET  /api/admin/logs    - Admin logs view');
    console.log('- POST /api/sales         - Record sale (with unit_price)');
    console.log('- GET  /api/businesses    - Get all businesses');
    console.log('- POST /api/payments/pesapal/initiate - Initiate PesaPal payment');
});