import { initializeApp, getApps } from 'firebase/app';
import { getFirestore, doc, getDoc, setDoc, deleteDoc, collection, getDocs } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: "AIzaSyDIuWwdeTOR6t_VxSv6w8i3Q4vYGW0p2sg",
  authDomain: "moviematcher-f4c5e.firebaseapp.com",
  projectId: "moviematcher-f4c5e",
  storageBucket: "moviematcher-f4c5e.firebasestorage.app",
  messagingSenderId: "337852118523",
  appId: "1:337852118523:web:dd021ee4442d936f8dc75e",
};

const app = getApps().length === 0 ? initializeApp(firebaseConfig) : getApps()[0];
const db = getFirestore(app);

export interface SavedUserState {
  userName: string;
  partyName: string;
  liked: { id: number; title: string }[];
  disliked: { id: number; title: string }[];
  activeGenres: string[];
  activeDecades: string[];
  genreWeights: Record<string, number>;
  decadeHints: string[];
  lam: number;
  prefIntensity: number;
  sortBy: string;
  sortDir: string;
  imdbMin: number | null;
  imdbMax: number | null;
  kFetch: number;
  steeringStrength: number;
  mixerMovies: { movieId: number; title: string; year?: number; poster_url?: string | null; weight: number }[];
  lastQuery: string;
  updatedAt: number;
}

function sanitize(s: string) {
  return s.toLowerCase().trim().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '');
}

function userRef(partyName: string, userName: string) {
  return doc(db, 'parties', sanitize(partyName), 'users', sanitize(userName));
}

export async function loadUser(partyName: string, userName: string): Promise<SavedUserState | null> {
  try {
    const snap = await getDoc(userRef(partyName, userName));
    return snap.exists() ? (snap.data() as SavedUserState) : null;
  } catch (e) {
    console.error('[Firebase] Load:', e);
    return null;
  }
}

export async function saveUser(partyName: string, userName: string, state: Partial<SavedUserState>): Promise<void> {
  try {
    await setDoc(userRef(partyName, userName), { userName, partyName, ...state, updatedAt: Date.now() }, { merge: true });
  } catch (e) {
    console.error('[Firebase] Save:', e);
  }
}

// Delete a specific user from a party (for cleanup)
export async function deleteUser(partyName: string, userName: string): Promise<void> {
  try {
    await deleteDoc(userRef(partyName, userName));
  } catch (e) {
    console.error('[Firebase] Delete:', e);
  }
}

// List all users in a party (for multiplayer later)
export async function listPartyUsers(partyName: string): Promise<SavedUserState[]> {
  try {
    const col = collection(db, 'parties', sanitize(partyName), 'users');
    const snap = await getDocs(col);
    return snap.docs.map(d => d.data() as SavedUserState);
  } catch (e) {
    console.error('[Firebase] List party:', e);
    return [];
  }
}

export async function deletePartyUsers(partyName: string): Promise<void> {
  try {
    const users = await listPartyUsers(partyName);
    await Promise.all(users.map(u => deleteUser(partyName, u.userName)));
  } catch (e) {
    console.error('[Firebase] Delete party users:', e);
  }
}