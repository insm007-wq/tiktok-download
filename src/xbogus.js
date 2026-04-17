/**
 * TikTok X-Bogus Signature Generator (Node.js)
 *
 * 사용법: node xbogus.js "<query_string>" "<user_agent>"
 * 출력:  X-Bogus 값 (stdout 한 줄)
 *
 * Python 포팅본이 막히는 이유:
 *   - Python에서 bytearray를 RC4에 넘길 때 ord() 처리 차이
 *   - base64.b64encode 결과를 bytes vs str로 md5에 넘기는 미묘한 차이
 * 이 파일은 JS 원본 알고리즘 로직 그대로 실행하므로 위 문제가 없음.
 */

'use strict';

const crypto = require('crypto');

const ALPHABET = 'Dkdpgh4ZKsQB80/Mfvw36XI1R25-WUAlEi7NLboqYTOPuzmFjJnryx9HVGcaStCe=';

/**
 * RC4 stream cipher
 * @param {string} key  - 키 (charCodeAt 으로 읽음)
 * @param {Buffer} data - 암호화/복호화할 데이터
 * @returns {Buffer}
 */
function rc4(key, data) {
  const S = Array.from({ length: 256 }, (_, i) => i);
  let j = 0;
  for (let i = 0; i < 256; i++) {
    j = (j + S[i] + key.charCodeAt(i % key.length)) % 256;
    [S[i], S[j]] = [S[j], S[i]];
  }

  let t = 0;
  j = 0;
  const out = Buffer.alloc(data.length);
  for (let i = 0; i < data.length; i++) {
    t = (t + 1) % 256;
    j = (j + S[t]) % 256;
    [S[t], S[j]] = [S[j], S[t]];
    out[i] = data[i] ^ S[(S[t] + S[j]) % 256];
  }
  return out;
}

/** double-MD5 → raw Buffer (16 bytes) */
function md5x2(input) {
  const first = crypto.createHash('md5').update(input).digest();
  return crypto.createHash('md5').update(first).digest();
}

/**
 * X-Bogus 서명 생성
 * @param {string} queryString - URL 쿼리 파라미터 문자열 (X-Bogus 제외)
 * @param {string} userAgent   - 요청에 사용할 User-Agent
 * @returns {string}
 */
function getXBogus(queryString, userAgent) {
  // ── 1. 각 재료의 MD5 salt 계산 ──────────────────────────────
  const saltPayload = md5x2(Buffer.from(queryString, 'utf8'));      // [16 bytes]
  const saltForm    = md5x2(Buffer.from('', 'utf8'));               // [16 bytes]

  // UA 는 RC4 암호화 후 Base64, 그 Base64 문자열을 단순 MD5 한 번
  const UA_KEY = '\x00\x01\x0e';                                    // == [0, 1, 14]
  const encUa  = rc4(UA_KEY, Buffer.from(userAgent, 'ascii'));
  const b64Ua  = encUa.toString('base64');
  const saltUa = crypto.createHash('md5').update(Buffer.from(b64Ua, 'ascii')).digest();

  // ── 2. arr1 구성 (18 바이트 + XOR 체크섬) ────────────────────
  const ts = Math.floor(Date.now() / 1000);

  const arr1 = [
    64, 0, 1, 14,
    saltPayload[14], saltPayload[15],
    saltForm[14],    saltForm[15],
    saltUa[14],      saltUa[15],
    (ts >>> 24) & 0xff, (ts >>> 16) & 0xff, (ts >>> 8) & 0xff, ts & 0xff,
    88, 194, 176, 26,
  ];
  arr1.push(arr1.reduce((a, b) => a ^ b, 0));  // XOR 체크섬 추가 → 19 bytes

  // ── 3. 짝수/홀수 인덱스 분리 → arr2 (10 + 9 = 19 bytes) ──────
  const even = arr1.filter((_, i) => i % 2 === 0);  // 10 bytes (i=0,2,...,18)
  const odd  = arr1.filter((_, i) => i % 2 === 1);  //  9 bytes (i=1,3,...,17)
  const arr2 = [...even, ...odd];

  // ── 4. 셔플 테이블 p 구성 (19 bytes) ─────────────────────────
  const p = [
    arr2[0],  arr2[10], arr2[1],  arr2[11], arr2[2],  arr2[12],
    arr2[3],  arr2[13], arr2[4],  arr2[14], arr2[5],  arr2[15],
    arr2[6],  arr2[16], arr2[7],  arr2[17], arr2[8],  arr2[18],
    arr2[9],
  ];

  // ── 5. RC4('\xff', p) → garbled (2 + 19 = 21 bytes) ──────────
  const pBuf    = Buffer.from(p);          // values 0-255, latin-1 safe
  const garbled = Buffer.concat([
    Buffer.from([2, 255]),
    rc4('\xff', pBuf),
  ]);

  // ── 6. Base64-like 인코딩 (7-bit 그룹 6개씩) ──────────────────
  let result = '';
  for (let i = 0; i < 21; i += 3) {
    const n = (garbled[i] << 16) | (garbled[i + 1] << 8) | garbled[i + 2];
    for (let shift = 18; shift >= 0; shift -= 6) {
      result += ALPHABET[(n >>> shift) & 63];
    }
  }

  return result;
}

// ── 진입점: 두 가지 모드 지원 ─────────────────────────────────────
//  1) CLI 한 번: node xbogus.js "<qs>" "<ua>"   → 한 줄 출력 후 종료 (기존 호환)
//  2) stdin 루프: node xbogus.js --serve        → 라인당 "qs\tua" 읽고 서명 한 줄씩 출력
//     Python 측에서 프로세스를 상주시켜 매 서명마다 node 기동 비용(~200ms) 제거.
if (require.main === module) {
  const args = process.argv.slice(2);
  if (args[0] === '--serve') {
    // 상주 모드: 한 줄 = 하나의 서명 요청.
    // 입력 형식: "<queryString>\t<userAgent>\n" (TAB 구분, UA에 탭이 들어올 일은 없음)
    // 출력 형식: "<xbogus>\n"  또는 실패 시 "ERR <메시지>\n"
    process.stdin.setEncoding('utf8');
    let buffer = '';
    process.stdin.on('data', (chunk) => {
      buffer += chunk;
      let nl;
      while ((nl = buffer.indexOf('\n')) !== -1) {
        const line = buffer.slice(0, nl);
        buffer = buffer.slice(nl + 1);
        if (!line) continue;
        const tab = line.indexOf('\t');
        if (tab < 0) {
          process.stdout.write('ERR missing_tab\n');
          continue;
        }
        const qs = line.slice(0, tab);
        const ua = line.slice(tab + 1);
        try {
          process.stdout.write(getXBogus(qs, ua) + '\n');
        } catch (e) {
          process.stdout.write('ERR ' + (e && e.message ? e.message : String(e)).replace(/\n/g, ' ') + '\n');
        }
      }
    });
    process.stdin.on('end', () => process.exit(0));
  } else {
    // 레거시 CLI 모드 (fallback)
    const [queryString, userAgent] = args;
    if (!queryString || !userAgent) {
      process.stderr.write('Usage: node xbogus.js "<query_string>" "<user_agent>" | node xbogus.js --serve\n');
      process.exit(1);
    }
    process.stdout.write(getXBogus(queryString, userAgent) + '\n');
  }
}

module.exports = { getXBogus };
