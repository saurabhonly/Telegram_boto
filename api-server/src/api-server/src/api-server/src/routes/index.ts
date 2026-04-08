import { Router } from 'express';
import healthRoute from './health';
import callbackRoute from './callback';

const router = Router();

router.use('/health', healthRoute);
router.use('/callback', callbackRoute);

export default router;
