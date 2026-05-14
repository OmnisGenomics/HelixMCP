export interface PngMetadata {
  format: "PNG";
  widthPx: number;
  heightPx: number;
  bitDepth: number;
  colorTypeCode: number;
  colorType: "grayscale" | "rgb" | "indexed" | "grayscale_alpha" | "rgba";
  channelCount: number;
  hasAlpha: boolean;
  interlaced: boolean;
  compressionMethod: number;
  filterMethod: number;
}

export const PNG_IHDR_BYTES = 29;

const PNG_SIGNATURE = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);

const COLOR_TYPES = new Map<
  number,
  {
    colorType: PngMetadata["colorType"];
    channelCount: number;
    hasAlpha: boolean;
    bitDepths: number[];
  }
>([
  [0, { colorType: "grayscale", channelCount: 1, hasAlpha: false, bitDepths: [1, 2, 4, 8, 16] }],
  [2, { colorType: "rgb", channelCount: 3, hasAlpha: false, bitDepths: [8, 16] }],
  [3, { colorType: "indexed", channelCount: 1, hasAlpha: false, bitDepths: [1, 2, 4, 8] }],
  [4, { colorType: "grayscale_alpha", channelCount: 2, hasAlpha: true, bitDepths: [8, 16] }],
  [6, { colorType: "rgba", channelCount: 4, hasAlpha: true, bitDepths: [8, 16] }]
]);

export function parsePngMetadata(buffer: Buffer): PngMetadata {
  if (buffer.length < PNG_IHDR_BYTES) {
    throw new Error(`invalid PNG: expected at least ${PNG_IHDR_BYTES} bytes, got ${buffer.length}`);
  }

  if (!buffer.subarray(0, PNG_SIGNATURE.length).equals(PNG_SIGNATURE)) {
    throw new Error("invalid PNG: signature mismatch");
  }

  const ihdrLength = buffer.readUInt32BE(8);
  if (ihdrLength !== 13) {
    throw new Error(`invalid PNG: IHDR length must be 13, got ${ihdrLength}`);
  }

  const chunkType = buffer.toString("ascii", 12, 16);
  if (chunkType !== "IHDR") {
    throw new Error(`invalid PNG: first chunk must be IHDR, got ${chunkType}`);
  }

  const widthPx = buffer.readUInt32BE(16);
  const heightPx = buffer.readUInt32BE(20);
  const bitDepth = buffer.readUInt8(24);
  const colorTypeCode = buffer.readUInt8(25);
  const compressionMethod = buffer.readUInt8(26);
  const filterMethod = buffer.readUInt8(27);
  const interlaceMethod = buffer.readUInt8(28);

  if (widthPx === 0 || heightPx === 0) {
    throw new Error("invalid PNG: width and height must be positive");
  }

  const colorInfo = COLOR_TYPES.get(colorTypeCode);
  if (!colorInfo) {
    throw new Error(`invalid PNG: unsupported color type ${colorTypeCode}`);
  }

  if (!colorInfo.bitDepths.includes(bitDepth)) {
    throw new Error(`invalid PNG: bit depth ${bitDepth} is not valid for color type ${colorTypeCode}`);
  }

  if (compressionMethod !== 0) {
    throw new Error(`invalid PNG: unsupported compression method ${compressionMethod}`);
  }

  if (filterMethod !== 0) {
    throw new Error(`invalid PNG: unsupported filter method ${filterMethod}`);
  }

  if (interlaceMethod !== 0 && interlaceMethod !== 1) {
    throw new Error(`invalid PNG: unsupported interlace method ${interlaceMethod}`);
  }

  return {
    format: "PNG",
    widthPx,
    heightPx,
    bitDepth,
    colorTypeCode,
    colorType: colorInfo.colorType,
    channelCount: colorInfo.channelCount,
    hasAlpha: colorInfo.hasAlpha,
    interlaced: interlaceMethod === 1,
    compressionMethod,
    filterMethod
  };
}
