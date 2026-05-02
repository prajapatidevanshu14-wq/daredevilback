const express = require('express'); 
const cors = require('cors');
const axios = require('axios');
const mongoose = require('mongoose');

const app = express();
const PORT = process.env.PORT || 5000; 

app.use(cors());
app.use(express.json());

/* =========================
   🔥 MONGODB CONNECTION
========================= */
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb+srv://jameswilliamsafm_db_user:n91a56EOL12yGewn@batman-hush.u4kvp7n.mongodb.net/?appName=batman-hush';

/* =========================
   🔥 MONGODB SCHEMAS
   ✅ FIX: index() AFTER schema definition
========================= */
const RunSchema = new mongoose.Schema({
  id: { type: Number, required: true, index: true },
  schedulerOrderId: { type: String, required: true, index: true },
  label: { type: String, required: true },
  apiUrl: { type: String, required: true },
  apiKey: { type: String, required: true },
  service: { type: String, required: true },
  link: { type: String, required: true },
  quantity: { type: Number, required: true },
  time: { type: Date, required: true },
  done: { type: Boolean, default: false },
  status: { type: String, default: 'pending', index: true },
  smmOrderId: { type: Number, default: null },
  createdAt: { type: Date, default: Date.now },
  executedAt: { type: Date, default: null },
  error: { type: String, default: null },
  comments: { type: String, default: null },
});

// ✅ FIX: index AFTER schema is defined
RunSchema.index({ status: 1, time: 1 });
RunSchema.index({ done: 1, status: 1, time: 1 });

const OrderSchema = new mongoose.Schema({
  schedulerOrderId: { type: String, required: true, unique: true, index: true },
  name: { type: String, required: true },
  link: { type: String, required: true },
  status: { type: String, default: 'pending' },
  totalRuns: { type: Number, required: true },
  completedRuns: { type: Number, default: 0 },
  runStatuses: [{ type: String }],
  createdAt: { type: Date, default: Date.now },
  lastUpdatedAt: { type: Date, default: Date.now },
});

const SettingsSchema = new mongoose.Schema({
  key: { type: String, required: true, unique: true },
  value: { type: mongoose.Schema.Types.Mixed, required: true },
  updatedAt: { type: Date, default: Date.now },
});

const Run = mongoose.model('Run', RunSchema);
const Order = mongoose.model('Order', OrderSchema);
const Settings = mongoose.model('Settings', SettingsSchema);

/* =========================
   MINIMUM VIEWS PER RUN
========================= */
let MIN_VIEWS_PER_RUN = 100;

async function loadSettings() {
  try {
    const setting = await Settings.findOne({ key: 'minViewsPerRun' });
    if (setting && typeof setting.value === 'number' && setting.value >= 1) {
      MIN_VIEWS_PER_RUN = setting.value;
      console.log(`✅ Loaded MIN_VIEWS_PER_RUN from DB: ${MIN_VIEWS_PER_RUN}`);
    } else {
      await Settings.findOneAndUpdate(
        { key: 'minViewsPerRun' },
        { key: 'minViewsPerRun', value: MIN_VIEWS_PER_RUN, updatedAt: new Date() },
        { upsert: true }
      );
      console.log(`✅ Saved default MIN_VIEWS_PER_RUN to DB: ${MIN_VIEWS_PER_RUN}`);
    }
  } catch (err) {
    console.error('Warning: Could not load settings from DB:', err.message);
  }
}

async function saveMinViewsSetting(value) {
  try {
    await Settings.findOneAndUpdate(
      { key: 'minViewsPerRun' },
      { key: 'minViewsPerRun', value, updatedAt: new Date() },
      { upsert: true }
    );
  } catch (err) {
    console.error('Warning: Could not save settings to DB:', err.message);
  }
}

/* =========================
   🔥 5 SEPARATE QUEUES + FLAGS
========================= */
let viewsQueue = [];
let likesQueue = [];
let sharesQueue = [];
let savesQueue = [];
let commentsQueue = [];

let isExecutingViews = false;
let isExecutingLikes = false;
let isExecutingShares = false;
let isExecutingSaves = false;
let isExecutingComments = false;

const lastExecutionTime = new Map();
const MIN_COOLDOWN_MS = 10 * 60 * 1000;

/* =========================
   🔥 START SERVER FIRST
========================= */
app.listen(PORT, '0.0.0.0', () => {
  console.log(`========================================`);
  console.log(`Server running on port ${PORT}`);
  console.log(`Minimum views per run: ${MIN_VIEWS_PER_RUN}`);
  console.log(`Scheduler runs every 10 seconds`);
  console.log(`========================================`);
});

/* =========================
   🔥 CONNECT MONGODB
========================= */
mongoose.connect(MONGODB_URI, {
  serverSelectionTimeoutMS: 30000,
})
.then(async () => {
  console.log('✅ MongoDB Connected Successfully');

  await loadSettings();

  try {
    const fifteenMinAgo = new Date(Date.now() - 15 * 60 * 1000);
    const cleanResult = await Run.updateMany(
      { 
        status: 'processing', 
        executedAt: null,
        createdAt: { $lt: fifteenMinAgo }
      },
      { $set: { status: 'pending', error: null } }
    );
    if (cleanResult.modifiedCount > 0) {
      console.log(`✅ Cleaned ${cleanResult.modifiedCount} stuck runs on startup`);
    }

    const queuedClean = await Run.updateMany(
      { status: 'queued' },
      { $set: { status: 'pending' } }
    );
    if (queuedClean.modifiedCount > 0) {
      console.log(`✅ Reset ${queuedClean.modifiedCount} queued runs to pending on startup`);
    }
  } catch (err) {
    console.error('Warning: Could not clean stuck runs:', err.message);
  }
})
.catch(err => {
  console.error('❌ MongoDB Connection Error:', err);
  console.log('⚠️ Server running but database not connected');
});

/* =========================
   PLACE ORDER
========================= */
async function placeOrder({ apiUrl, apiKey, service, link, quantity, comments }) {
  const params = new URLSearchParams({
    key: apiKey,
    action: 'add',
    service: String(service),
    link: String(link),
    quantity: String(quantity),
  });

  if (comments) {
    params.append('comments', comments);
  }

  const response = await axios.post(apiUrl, params.toString(), {
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    timeout: 30000,
  });

  return response.data;
}

/* =========================
   ADD RUNS TO DATABASE
========================= */
async function addRuns(services, baseConfig, schedulerOrderId) {
  const runsForOrder = [];

  for (const [key, serviceConfig] of Object.entries(services)) {
    if (!serviceConfig) continue;

    const label = key.toUpperCase();

    for (const run of serviceConfig.runs) {
      let quantity;

      if (label === 'VIEWS') {
        if (!run.quantity || run.quantity < MIN_VIEWS_PER_RUN) {
          console.log(`[SKIP VIEWS RUN] quantity ${run.quantity} < MIN_VIEWS_PER_RUN ${MIN_VIEWS_PER_RUN}`);
          continue;
        }
        quantity = run.quantity;
      } else if (label === 'COMMENTS') {
        if (!run.comments) continue;

        let lines = run.comments
          .split('\n')
          .map(c => c.trim())
          .filter(c => c.length > 0);

        if (lines.length < 1) continue;

        if (lines.length > 10) {
          lines = lines.sort(() => Math.random() - 0.5).slice(0, 10);
        }

        run.comments = lines.join('\n');
        quantity = lines.length;
      } else {
        if (!run.quantity || run.quantity <= 0) continue;
        quantity = run.quantity;
      }

      let scheduledTime;
      try {
        scheduledTime = new Date(run.time);
        if (isNaN(scheduledTime.getTime())) {
          console.error(`[ADD RUNS] Invalid time for run: ${run.time}, skipping`);
          continue;
        }
        const nowMs = Date.now();
        if (scheduledTime.getTime() < nowMs) {
          const isPastByMoreThan5Min = (nowMs - scheduledTime.getTime()) > 5 * 60 * 1000;
          if (isPastByMoreThan5Min) {
            console.log(`[ADD RUNS] Skipping past run scheduled at ${scheduledTime.toISOString()}`);
            continue;
          }
        }
      } catch (err) {
        console.error(`[ADD RUNS] Error parsing time: ${run.time}`, err);
        continue;
      }

      const runData = new Run({
        id: Date.now() + Math.random(),
        schedulerOrderId,
        label,
        apiUrl: baseConfig.apiUrl,
        apiKey: baseConfig.apiKey,
        service: serviceConfig.serviceId,
        link: baseConfig.link,
        quantity: quantity,
        time: scheduledTime,
        done: false,
        status: 'pending',
        smmOrderId: null,
        createdAt: new Date(),
        executedAt: null,
        error: null,
        comments: run.comments || null,
      });

      await runData.save();
      runsForOrder.push(runData);
    }
  }

  return runsForOrder;
}

/* =========================
   EXECUTE RUN
========================= */
async function executeRun(run) {
  const order = await Order.findOne({ schedulerOrderId: run.schedulerOrderId });
  if (run.status === 'cancelled') {
    console.log(`[SKIP] Run already cancelled`);
    return;
  }
  if (!order || order.status === 'cancelled') {
    console.log(`[SKIP] Order cancelled → run skipped`);
    return;
  }

  try {
    const fifteenMinAgo = new Date(Date.now() - 15 * 60 * 1000);
    await Run.updateMany(
      { status: 'processing', executedAt: null, createdAt: { $lt: fifteenMinAgo } },
      { $set: { status: 'failed', error: 'Stuck run cleaned up' } }
    );

    const activeSameType = await Run.findOne({
      link: run.link,
      label: run.label,
      status: { $in: ['processing'] },
      _id: { $ne: run._id }
    });

    if (activeSameType) {
      console.log(`[${run.label}] Skipping - same type already processing for this link`);
      if (run.label === 'VIEWS') viewsQueue.push(run);
      if (run.label === 'LIKES') likesQueue.push(run);
      if (run.label === 'SHARES') sharesQueue.push(run);
      if (run.label === 'SAVES') savesQueue.push(run);
      if (run.label === 'COMMENTS') commentsQueue.push(run);
      return;
    }

    if (!run || !run._id) {
      console.warn(`[${run?.label}] Invalid run, skipping`);
      return;
    }

    if (!run.quantity || run.quantity <= 0) return;

    if (run.label === 'VIEWS' && run.quantity < MIN_VIEWS_PER_RUN) {
      console.log(`[VIEWS] Skipping run with quantity ${run.quantity} < MIN_VIEWS_PER_RUN ${MIN_VIEWS_PER_RUN}`);
      await Run.updateOne(
        { _id: run._id },
        { $set: { status: 'cancelled', error: `Below minimum views per run (${MIN_VIEWS_PER_RUN})` } }
      );
      await updateOrderStatus(run.schedulerOrderId);
      return;
    }

    console.log(`[${run.label}] Executing run #${run.id}, quantity: ${run.quantity}`);

    await Run.updateOne(
      { _id: run._id },
      { $set: { status: 'processing' } }
    );

    await updateOrderStatus(run.schedulerOrderId);

    let payload = {
      apiUrl: run.apiUrl,
      apiKey: run.apiKey,
      service: run.service,
      link: run.link,
    };

    if (run.label === 'COMMENTS') {
      payload.comments = run.comments;
      payload.quantity = run.quantity;
    } else {
      payload.quantity = run.quantity;
    }

    const result = await placeOrder(payload);

    if (result?.order) {
      console.log(`[${run.label}] SUCCESS - SMM Order ID: ${result.order}`);
      await Run.updateOne(
        { _id: run._id },
        {
          $set: {
            done: true,
            status: 'completed',
            smmOrderId: result.order,
            executedAt: new Date(),
          }
        }
      );
    } else {
      console.error(`[${run.label}] FAILED`, result);
      const errorMsg = result?.error || 'Unknown error';

      if (errorMsg.toLowerCase().includes('active order') ||
          errorMsg.toLowerCase().includes('wait until')) {
        console.log(`[${run.label}] Provider busy - resetting to pending, retry in 5 min`);
        await Run.updateOne(
          { _id: run._id },
          {
            $set: {
              status: 'pending',
              error: null,
              time: new Date(Date.now() + 5 * 60 * 1000)
            }
          }
        );
      } else {
        await Run.updateOne(
          { _id: run._id },
          { $set: { status: 'failed', error: errorMsg } }
        );
      }
    }

  } catch (err) {
    console.error(`[${run.label}] ERROR`, err.response?.data || err.message);
    const errorMsg = err.response?.data?.error || err.message;

    if (run?._id) {
      if (errorMsg?.toLowerCase?.()?.includes?.('active order') ||
          errorMsg?.toLowerCase?.()?.includes?.('wait until')) {
        console.log(`[${run.label}] Provider busy (catch) - resetting to pending`);
        await Run.updateOne(
          { _id: run._id },
          {
            $set: {
              status: 'pending',
              error: null,
              time: new Date(Date.now() + 5 * 60 * 1000)
            }
          }
        );
      } else {
        await Run.updateOne(
          { _id: run._id },
          { $set: { status: 'failed', error: errorMsg } }
        );
      }
    }
  }

  await updateOrderStatus(run.schedulerOrderId);
}

/* =========================
   UPDATE ORDER STATUS
========================= */
async function updateOrderStatus(schedulerOrderId) {
  if (!schedulerOrderId) return;

  // ✅ Use .lean() here - we only need plain data for counting
  const orderRuns = await Run.find({ schedulerOrderId }).lean();
  const order = await Order.findOne({ schedulerOrderId });

  if (!order) return;
  if (order.status === 'cancelled') return;

  const totalRuns = orderRuns.length;
  const completedRuns = orderRuns.filter(r => r.status === 'completed').length;
  const failedRuns = orderRuns.filter(r => r.status === 'failed').length;
  const cancelledRuns = orderRuns.filter(r => r.status === 'cancelled').length;
  const processingRuns = orderRuns.filter(r => r.status === 'processing').length;
  const queuedRuns = orderRuns.filter(r => r.status === 'queued').length;
  const pausedRuns = orderRuns.filter(r => r.status === 'paused').length;
  const pendingRuns = orderRuns.filter(r => r.status === 'pending').length;

  const activeRuns = totalRuns - cancelledRuns;

  let newStatus;

  if (activeRuns === 0) {
    newStatus = 'cancelled';
  } else if (completedRuns === activeRuns) {
    newStatus = 'completed';
  } else if (failedRuns === activeRuns) {
    newStatus = 'failed';
  } else if (pausedRuns > 0 && processingRuns === 0 && queuedRuns === 0) {
    newStatus = 'paused';
  } else if (processingRuns > 0 || completedRuns > 0 || queuedRuns > 0) {
    newStatus = 'running';
  } else if (pendingRuns > 0) {
    newStatus = 'pending';
  } else {
    newStatus = order.status;
  }

  await Order.updateOne(
    { schedulerOrderId },
    {
      $set: {
        status: newStatus,
        completedRuns,
        totalRuns,
        lastUpdatedAt: new Date(),
        runStatuses: orderRuns.map(r => r.status)
      }
    }
  );
}

/* =========================
   🔥 QUEUE PROCESSORS
========================= */
async function processViewsQueue() {
  if (isExecutingViews || viewsQueue.length === 0) return;

  isExecutingViews = true;
  const run = viewsQueue.shift();

  console.log(`[VIEWS QUEUE] Processing run #${run.id}, Remaining: ${viewsQueue.length}`);

  try {
    const cooldownKey = `${run.link}-${run.label}`;
    const lastExec = lastExecutionTime.get(cooldownKey) || 0;
    const timeSinceLast = Date.now() - lastExec;

    if (timeSinceLast < MIN_COOLDOWN_MS) {
      const waitMs = MIN_COOLDOWN_MS - timeSinceLast;
      console.log(`[VIEWS QUEUE] Cooldown active, waiting ${Math.round(waitMs/1000)}s`);
      viewsQueue.unshift(run);
      isExecutingViews = false;
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 60000)));
      if (viewsQueue.length > 0) setImmediate(() => processViewsQueue());
      return;
    }

    // ✅ Fetch fresh mongoose document (not lean) so we can update it
    const freshRun = await Run.findById(run._id);
    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[VIEWS QUEUE] Skipped cancelled run`);
    } else {
      lastExecutionTime.set(cooldownKey, Date.now());
      await executeRun(freshRun);
    }
  } catch (err) {
    console.error(`[VIEWS QUEUE] Error:`, err);
  }

  isExecutingViews = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (viewsQueue.length > 0) {
    setImmediate(() => processViewsQueue());
  }
}

async function processLikesQueue() {
  if (isExecutingLikes || likesQueue.length === 0) return;

  isExecutingLikes = true;
  const run = likesQueue.shift();

  console.log(`[LIKES QUEUE] Processing run #${run.id}, Remaining: ${likesQueue.length}`);

  try {
    const cooldownKey = `${run.link}-${run.label}`;
    const lastExec = lastExecutionTime.get(cooldownKey) || 0;
    const timeSinceLast = Date.now() - lastExec;

    if (timeSinceLast < MIN_COOLDOWN_MS) {
      const waitMs = MIN_COOLDOWN_MS - timeSinceLast;
      console.log(`[LIKES QUEUE] Cooldown active, waiting ${Math.round(waitMs/1000)}s`);
      likesQueue.unshift(run);
      isExecutingLikes = false;
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 60000)));
      if (likesQueue.length > 0) setImmediate(() => processLikesQueue());
      return;
    }

    const freshRun = await Run.findById(run._id);
    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[LIKES QUEUE] Skipped cancelled run`);
    } else {
      lastExecutionTime.set(cooldownKey, Date.now());
      await executeRun(freshRun);
    }
  } catch (err) {
    console.error(`[LIKES QUEUE] Error:`, err);
  }

  isExecutingLikes = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (likesQueue.length > 0) {
    setImmediate(() => processLikesQueue());
  }
}

async function processSharesQueue() {
  if (isExecutingShares || sharesQueue.length === 0) return;

  isExecutingShares = true;
  const run = sharesQueue.shift();

  console.log(`[SHARES QUEUE] Processing run #${run.id}, Remaining: ${sharesQueue.length}`);

  try {
    const cooldownKey = `${run.link}-${run.label}`;
    const lastExec = lastExecutionTime.get(cooldownKey) || 0;
    const timeSinceLast = Date.now() - lastExec;

    if (timeSinceLast < MIN_COOLDOWN_MS) {
      const waitMs = MIN_COOLDOWN_MS - timeSinceLast;
      console.log(`[SHARES QUEUE] Cooldown active, waiting ${Math.round(waitMs/1000)}s`);
      sharesQueue.unshift(run);
      isExecutingShares = false;
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 60000)));
      if (sharesQueue.length > 0) setImmediate(() => processSharesQueue());
      return;
    }

    const freshRun = await Run.findById(run._id);
    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[SHARES QUEUE] Skipped cancelled run`);
    } else {
      lastExecutionTime.set(cooldownKey, Date.now());
      await executeRun(freshRun);
    }
  } catch (err) {
    console.error(`[SHARES QUEUE] Error:`, err);
  }

  isExecutingShares = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (sharesQueue.length > 0) {
    setImmediate(() => processSharesQueue());
  }
}

async function processSavesQueue() {
  if (isExecutingSaves || savesQueue.length === 0) return;

  isExecutingSaves = true;
  const run = savesQueue.shift();

  console.log(`[SAVES QUEUE] Processing run #${run.id}, Remaining: ${savesQueue.length}`);

  try {
    const cooldownKey = `${run.link}-${run.label}`;
    const lastExec = lastExecutionTime.get(cooldownKey) || 0;
    const timeSinceLast = Date.now() - lastExec;

    if (timeSinceLast < MIN_COOLDOWN_MS) {
      const waitMs = MIN_COOLDOWN_MS - timeSinceLast;
      console.log(`[SAVES QUEUE] Cooldown active, waiting ${Math.round(waitMs/1000)}s`);
      savesQueue.unshift(run);
      isExecutingSaves = false;
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 60000)));
      if (savesQueue.length > 0) setImmediate(() => processSavesQueue());
      return;
    }

    const freshRun = await Run.findById(run._id);
    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[SAVES QUEUE] Skipped cancelled run`);
    } else {
      lastExecutionTime.set(cooldownKey, Date.now());
      await executeRun(freshRun);
    }
  } catch (err) {
    console.error(`[SAVES QUEUE] Error:`, err);
  }

  isExecutingSaves = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (savesQueue.length > 0) {
    setImmediate(() => processSavesQueue());
  }
}

async function processCommentsQueue() {
  if (isExecutingComments || commentsQueue.length === 0) return;

  isExecutingComments = true;
  const run = commentsQueue.shift();

  console.log(`[COMMENTS QUEUE] Processing run #${run.id}, Remaining: ${commentsQueue.length}`);

  try {
    const cooldownKey = `${run.link}-${run.label}`;
    const lastExec = lastExecutionTime.get(cooldownKey) || 0;
    const timeSinceLast = Date.now() - lastExec;

    if (timeSinceLast < MIN_COOLDOWN_MS) {
      const waitMs = MIN_COOLDOWN_MS - timeSinceLast;
      console.log(`[COMMENTS QUEUE] Cooldown active, waiting ${Math.round(waitMs/1000)}s`);
      commentsQueue.unshift(run);
      isExecutingComments = false;
      await new Promise(resolve => setTimeout(resolve, Math.min(waitMs, 60000)));
      if (commentsQueue.length > 0) setImmediate(() => processCommentsQueue());
      return;
    }

    const freshRun = await Run.findById(run._id);
    if (!freshRun || freshRun.status === 'cancelled') {
      console.log(`[COMMENTS QUEUE] Skipped cancelled run`);
    } else {
      lastExecutionTime.set(cooldownKey, Date.now());
      await executeRun(freshRun);
    }
  } catch (err) {
    console.error(`[COMMENTS QUEUE] Error:`, err);
  }

  isExecutingComments = false;
  await new Promise(resolve => setTimeout(resolve, 8000));

  if (commentsQueue.length > 0) {
    setImmediate(() => processCommentsQueue());
  }
}

/* =========================
   CHECK IF RUN IN QUEUE
========================= */
function isRunInQueue(runId) {
  return viewsQueue.some(r => r.id === runId) ||
         likesQueue.some(r => r.id === runId) ||
         sharesQueue.some(r => r.id === runId) ||
         savesQueue.some(r => r.id === runId) ||
         commentsQueue.some(r => r.id === runId);
}

/* =========================
   🔥 MAIN SCHEDULER
   ✅ FIX: .lean() + .limit() for memory
   ✅ FIX: updateOne instead of run.save()
========================= */
mongoose.connection.once('open', () => {
  console.log("🚀 Scheduler started after DB connected");

  setInterval(async () => {
    try {
      const now = new Date();
      const nowMs = now.getTime();

      let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

      // ✅ FIX: Only load runs due NOW, limit 200, use lean()
      const allRuns = await Run.find({
        done: false,
        status: 'pending',
        time: { $lte: now }
      })
      .limit(200)
      .lean(); // ✅ plain objects - memory safe

      for (let run of allRuns) {
        if (isRunInQueue(run.id)) continue;

        // ✅ Use lean for order check too
        const order = await Order.findOne(
          { schedulerOrderId: run.schedulerOrderId },
          { status: 1 }  // only fetch status field
        ).lean();

        if (!order || order.status === 'cancelled') continue;

        // Validate run time
        let runTimeMs;
        try {
          runTimeMs = run.time instanceof Date
            ? run.time.getTime()
            : new Date(run.time).getTime();

          if (isNaN(runTimeMs)) {
            console.error(`[SCHEDULER] Invalid run time for run #${run.id}: ${run.time}`);
            continue;
          }
        } catch (err) {
          console.error(`[SCHEDULER] Error parsing run time for run #${run.id}:`, err);
          continue;
        }

        const isTimeReached = runTimeMs <= (nowMs + 1000);

        if (isTimeReached) {

          // Check minimum views
          if (run.label === 'VIEWS' && run.quantity < MIN_VIEWS_PER_RUN) {
            console.log(`[SCHEDULER] Skipping VIEWS run #${run.id} - quantity ${run.quantity} < MIN ${MIN_VIEWS_PER_RUN}`);
            // ✅ FIX: updateOne instead of run.save() (lean objects have no save())
            await Run.updateOne(
              { _id: run._id },
              { $set: { status: 'cancelled', done: true, error: `Below minimum views per run (${MIN_VIEWS_PER_RUN})` } }
            );
            continue;
          }

          // ✅ FIX: updateOne instead of run.save()
          await Run.updateOne(
            { _id: run._id },
            { $set: { status: 'queued' } }
          );

          if (run.label === 'VIEWS') {
            viewsQueue.push(run);
            addedToQueue.views++;
            console.log(`[SCHEDULER] Added VIEWS run #${run.id} to queue (qty: ${run.quantity})`);
          } else if (run.label === 'LIKES') {
            likesQueue.push(run);
            addedToQueue.likes++;
            console.log(`[SCHEDULER] Added LIKES run #${run.id} to queue (qty: ${run.quantity})`);
          } else if (run.label === 'SHARES') {
            sharesQueue.push(run);
            addedToQueue.shares++;
            console.log(`[SCHEDULER] Added SHARES run #${run.id} to queue (qty: ${run.quantity})`);
          } else if (run.label === 'SAVES') {
            savesQueue.push(run);
            addedToQueue.saves++;
            console.log(`[SCHEDULER] Added SAVES run #${run.id} to queue (qty: ${run.quantity})`);
          } else if (run.label === 'COMMENTS') {
            commentsQueue.push(run);
            addedToQueue.comments++;
            console.log(`[SCHEDULER] Added COMMENTS run #${run.id} to queue (qty: ${run.quantity})`);
          }
        }
      }

      if (addedToQueue.views + addedToQueue.likes + addedToQueue.shares + addedToQueue.saves + addedToQueue.comments > 0) {
        console.log(`[SCHEDULER] Added - Views: ${addedToQueue.views}, Likes: ${addedToQueue.likes}, Shares: ${addedToQueue.shares}, Saves: ${addedToQueue.saves}, Comments: ${addedToQueue.comments}`);
      }

      if (viewsQueue.length > 0 && !isExecutingViews) processViewsQueue();
      if (likesQueue.length > 0 && !isExecutingLikes) processLikesQueue();
      if (sharesQueue.length > 0 && !isExecutingShares) processSharesQueue();
      if (savesQueue.length > 0 && !isExecutingSaves) processSavesQueue();
      if (commentsQueue.length > 0 && !isExecutingComments) processCommentsQueue();

    } catch (error) {
      console.error('[SCHEDULER] Error:', error);
    }
  }, 10000);
});

/* =========================
   API ENDPOINTS
========================= */
app.post('/api/order', async (req, res) => {
  try {
    const { apiUrl, apiKey, link, services, name } = req.body;
    console.log("SERVICES RECEIVED:", JSON.stringify(services, null, 2));

    if (!apiUrl || !apiKey || !link || !services) {
      return res.status(400).json({ error: 'Missing required fields' });
    }

    const schedulerOrderId = `sched-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    const runsForOrder = await addRuns(services, { apiUrl, apiKey, link }, schedulerOrderId);

    const orderData = new Order({
      schedulerOrderId,
      name: name || `Order ${schedulerOrderId}`,
      link,
      status: 'pending',
      totalRuns: runsForOrder.length,
      completedRuns: 0,
      runStatuses: runsForOrder.map(() => 'pending'),
      createdAt: new Date(),
      lastUpdatedAt: new Date(),
    });

    await orderData.save();

    console.log(`Order created: ${schedulerOrderId} with ${runsForOrder.length} runs`);

    return res.json({
      success: true,
      message: 'Order scheduled (persistent)',
      schedulerOrderId,
      status: 'pending',
      completedRuns: 0,
      totalRuns: runsForOrder.length,
    });
  } catch (error) {
    console.error('[CREATE ORDER] Error:', error);
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/services', async (req, res) => {
  const { apiUrl, apiKey } = req.body;
  if (!apiUrl || !apiKey) {
    return res.status(400).json({ error: 'Missing API URL or key' });
  }
  try {
    const params = new URLSearchParams({ key: apiKey, action: 'services' });
    const response = await axios.post(apiUrl, params.toString(), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 30000,
    });
    return res.json(response.data);
  } catch (error) {
    return res.status(500).json({ error: error.response?.data || error.message });
  }
});

app.get('/api/order/status/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const order = await Order.findOne({ schedulerOrderId }).lean();
    const orderRuns = await Run.find({ schedulerOrderId }).lean();

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    return res.json({
      schedulerOrderId: order.schedulerOrderId,
      name: order.name,
      link: order.link,
      status: order.status,
      totalRuns: order.totalRuns,
      completedRuns: order.completedRuns,
      runStatuses: order.runStatuses,
      createdAt: order.createdAt,
      lastUpdatedAt: order.lastUpdatedAt,
      runs: orderRuns.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error: r.error,
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/orders/status', async (req, res) => {
  try {
    // ✅ Use lean() for read-only endpoints
    const allOrders = await Order.find().sort({ createdAt: -1 }).lean();
    const ordersWithRuns = await Promise.all(allOrders.map(async (order) => {
      const orderRuns = await Run.find(
        { schedulerOrderId: order.schedulerOrderId },
        { id: 1, label: 1, quantity: 1, time: 1, status: 1, smmOrderId: 1 }
      ).lean();
      return {
        schedulerOrderId: order.schedulerOrderId,
        name: order.name,
        link: order.link,
        status: order.status,
        totalRuns: order.totalRuns,
        completedRuns: order.completedRuns,
        runStatuses: order.runStatuses,
        createdAt: order.createdAt,
        lastUpdatedAt: order.lastUpdatedAt,
        runs: orderRuns,
      };
    }));

    return res.json({ total: allOrders.length, orders: ordersWithRuns });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/order/control', async (req, res) => {
  try {
    const { schedulerOrderId, action } = req.body;
    if (!schedulerOrderId || !action) {
      return res.status(400).json({ error: 'Missing schedulerOrderId or action' });
    }

    const order = await Order.findOne({ schedulerOrderId });
    const orderRuns = await Run.find({ schedulerOrderId });

    if (!order) {
      return res.status(404).json({ error: 'Order not found' });
    }

    if (action === 'cancel') {
      for (let run of orderRuns) {
        if (run.status === 'pending' || run.status === 'processing' || run.status === 'queued') {
          run.status = 'cancelled';
          run.done = true;
          await run.save();

          viewsQueue = viewsQueue.filter(r => r.id !== run.id);
          likesQueue = likesQueue.filter(r => r.id !== run.id);
          sharesQueue = sharesQueue.filter(r => r.id !== run.id);
          savesQueue = savesQueue.filter(r => r.id !== run.id);
          commentsQueue = commentsQueue.filter(r => r.id !== run.id);
        }
      }
      order.status = 'cancelled';
      await order.save();

      return res.json({
        success: true,
        status: 'cancelled',
        completedRuns: orderRuns.filter(r => r.status === 'completed').length,
        runStatuses: orderRuns.map(r => r.status),
      });
    }

    if (action === 'pause') {
      for (let run of orderRuns) {
        if (run.status === 'pending' || run.status === 'queued') {
          run.status = 'paused';
          await run.save();

          viewsQueue = viewsQueue.filter(r => r.id !== run.id);
          likesQueue = likesQueue.filter(r => r.id !== run.id);
          sharesQueue = sharesQueue.filter(r => r.id !== run.id);
          savesQueue = savesQueue.filter(r => r.id !== run.id);
          commentsQueue = commentsQueue.filter(r => r.id !== run.id);
        }
      }
      order.status = 'paused';
      await order.save();

      return res.json({
        success: true,
        status: 'paused',
        completedRuns: orderRuns.filter(r => r.status === 'completed').length,
        runStatuses: orderRuns.map(r => r.status),
      });
    }

    if (action === 'resume') {
      for (let run of orderRuns) {
        if (run.status === 'paused') {
          run.status = 'pending';
          await run.save();
        }
      }
      order.status = 'running';
      await order.save();

      return res.json({
        success: true,
        status: 'running',
        completedRuns: orderRuns.filter(r => r.status === 'completed').length,
        runStatuses: orderRuns.map(r => r.status),
      });
    }

    return res.status(400).json({ error: 'Invalid action' });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/order/runs/:schedulerOrderId', async (req, res) => {
  try {
    const { schedulerOrderId } = req.params;
    const orderRuns = await Run.find({ schedulerOrderId }).lean();
    return res.json({
      schedulerOrderId,
      runs: orderRuns.map(r => ({
        id: r.id,
        label: r.label,
        quantity: r.quantity,
        time: r.time,
        status: r.status,
        smmOrderId: r.smmOrderId,
        executedAt: r.executedAt,
        error: r.error,
      })),
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/settings/min-views', async (req, res) => {
  return res.json({ minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.post('/api/settings/min-views', async (req, res) => {
  const { minViewsPerRun } = req.body;
  if (typeof minViewsPerRun !== 'number' || minViewsPerRun < 1) {
    return res.status(400).json({ error: 'Invalid minViewsPerRun value' });
  }
  MIN_VIEWS_PER_RUN = Math.floor(minViewsPerRun);
  await saveMinViewsSetting(MIN_VIEWS_PER_RUN);
  console.log(`Minimum views per run updated to: ${MIN_VIEWS_PER_RUN}`);
  return res.json({ success: true, minViewsPerRun: MIN_VIEWS_PER_RUN });
});

app.get('/api/queues/status', (req, res) => {
  return res.json({
    views: { queueLength: viewsQueue.length, isExecuting: isExecutingViews },
    likes: { queueLength: likesQueue.length, isExecuting: isExecutingLikes },
    shares: { queueLength: sharesQueue.length, isExecuting: isExecutingShares },
    saves: { queueLength: savesQueue.length, isExecuting: isExecutingSaves },
    comments: { queueLength: commentsQueue.length, isExecuting: isExecutingComments },
    minViewsPerRun: MIN_VIEWS_PER_RUN,
  });
});

app.post('/api/runs/retry-stuck', async (req, res) => {
  try {
    let resetCount = 0;

    // ✅ Use updateMany instead of loading all runs into memory
    const queuedResult = await Run.updateMany(
      { status: 'queued', done: false },
      { $set: { status: 'pending' } }
    );
    resetCount += queuedResult.modifiedCount;

    return res.json({
      success: true,
      resetCount,
      message: `Reset ${resetCount} stuck runs`
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/scheduler/trigger', async (req, res) => {
  try {
    const now = new Date();
    const nowMs = now.getTime();
    let addedToQueue = { views: 0, likes: 0, shares: 0, saves: 0, comments: 0 };

    // ✅ Same memory-safe query as main scheduler
    const allRuns = await Run.find({
      done: false,
      status: 'pending',
      time: { $lte: now }
    })
    .limit(200)
    .lean();

    for (let run of allRuns) {
      if (isRunInQueue(run.id)) continue;

      const runTimeMs = run.time instanceof Date
        ? run.time.getTime()
        : new Date(run.time).getTime();

      if (runTimeMs <= (nowMs + 1000)) {

        if (run.label === 'VIEWS' && run.quantity < MIN_VIEWS_PER_RUN) {
          await Run.updateOne(
            { _id: run._id },
            { $set: { status: 'cancelled', done: true, error: `Below minimum views per run (${MIN_VIEWS_PER_RUN})` } }
          );
          continue;
        }

        // ✅ FIX: updateOne instead of run.save()
        await Run.updateOne(
          { _id: run._id },
          { $set: { status: 'queued' } }
        );

        if (run.label === 'VIEWS') { viewsQueue.push(run); addedToQueue.views++; }
        else if (run.label === 'LIKES') { likesQueue.push(run); addedToQueue.likes++; }
        else if (run.label === 'SHARES') { sharesQueue.push(run); addedToQueue.shares++; }
        else if (run.label === 'SAVES') { savesQueue.push(run); addedToQueue.saves++; }
        else if (run.label === 'COMMENTS') { commentsQueue.push(run); addedToQueue.comments++; }
      }
    }

    if (viewsQueue.length > 0 && !isExecutingViews) processViewsQueue();
    if (likesQueue.length > 0 && !isExecutingLikes) processLikesQueue();
    if (sharesQueue.length > 0 && !isExecutingShares) processSharesQueue();
    if (savesQueue.length > 0 && !isExecutingSaves) processSavesQueue();
    if (commentsQueue.length > 0 && !isExecutingComments) processCommentsQueue();

    return res.json({
      success: true,
      addedToQueue,
      minViewsPerRun: MIN_VIEWS_PER_RUN,
      currentQueues: {
        views: viewsQueue.length,
        likes: likesQueue.length,
        shares: sharesQueue.length,
        saves: savesQueue.length,
        comments: commentsQueue.length
      }
    });
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/health', (req, res) => {
  return res.json({
    status: 'ok',
    mongoConnected: mongoose.connection.readyState === 1,
    uptime: process.uptime(),
    minViewsPerRun: MIN_VIEWS_PER_RUN,
    queues: {
      views: viewsQueue.length,
      likes: likesQueue.length,
      shares: sharesQueue.length,
      saves: savesQueue.length,
      comments: commentsQueue.length
    }
  });
});

/* =========================
   KEEP ALIVE PING
========================= */
setInterval(async () => {
  try {
    await axios.get("https://batman-hush-backend.onrender.com/api/health");
    console.log("[PING] Keeping server alive");
  } catch (e) {}
}, 5 * 60 * 1000);
