package lsm.engine;

import lsm.common.Hex;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

final class Manifest {
    record Entry(String sstableRelPath, String bloomRelPath, byte[] minKey, byte[] maxKey) {}

    private Manifest() {}

    static Path manifestPath(Path dataDir) {
        return dataDir.resolve("manifest.json");
    }

    static List<Entry> load(Path dataDir) throws IOException {
        Path p = manifestPath(dataDir);
        if (!Files.exists(p)) return List.of();
        String json = Files.readString(p, StandardCharsets.UTF_8).trim();
        if (json.isEmpty() || json.equals("{}")) return List.of();

        // Minimal, strict parser for the format we write. Not a general JSON parser.
        List<Entry> out = new ArrayList<>();
        int idx = json.indexOf("\"sstables\"");
        if (idx < 0) return List.of();
        int arrStart = json.indexOf('[', idx);
        int arrEnd = json.indexOf(']', arrStart);
        if (arrStart < 0 || arrEnd < 0) return List.of();
        String arr = json.substring(arrStart + 1, arrEnd);
        String[] objs = arr.split("\\},\\s*\\{");
        for (String raw : objs) {
            String o = raw.trim();
            if (o.isEmpty()) continue;
            o = o.replace("{", "").replace("}", "").trim();
            String file = extract(o, "\"file\":\"", "\"");
            String bloom = extract(o, "\"bloom\":\"", "\"");
            String min = extract(o, "\"minKey\":\"", "\"");
            String max = extract(o, "\"maxKey\":\"", "\"");
            if (file == null) continue;
            out.add(new Entry(file, bloom, Hex.fromHex(min == null ? "" : min), Hex.fromHex(max == null ? "" : max)));
        }
        return out;
    }

    static void store(Path dataDir, List<Entry> entries) throws IOException {
        Path p = manifestPath(dataDir);
        Files.createDirectories(p.toAbsolutePath().getParent());
        Path tmp = p.resolveSibling(p.getFileName().toString() + ".tmp");

        StringBuilder sb = new StringBuilder();
        sb.append("{\"sstables\":[");
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            if (i > 0) sb.append(',');
            sb.append("{\"file\":\"").append(e.sstableRelPath).append("\"");
            if (e.bloomRelPath != null) sb.append(",\"bloom\":\"").append(e.bloomRelPath).append("\"");
            sb.append(",\"minKey\":\"").append(Hex.toHex(e.minKey)).append("\"");
            sb.append(",\"maxKey\":\"").append(Hex.toHex(e.maxKey)).append("\"");
            sb.append('}');
        }
        sb.append("]}");
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
        Files.move(tmp, p, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String extract(String obj, String prefix, String endQuote) {
        int s = obj.indexOf(prefix);
        if (s < 0) return null;
        s += prefix.length();
        int e = obj.indexOf(endQuote, s);
        if (e < 0) return null;
        return obj.substring(s, e);
    }
}

