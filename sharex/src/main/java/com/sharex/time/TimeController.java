package com.sharex.time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/time")
@CrossOrigin(origins = "*")
public class TimeController {
    private static final Logger logger = LoggerFactory.getLogger(TimeController.class);

    @Autowired
    private TimeSyncService timeSyncService;

    /**
     * Follower provides its physical time to the leader.
     * The Berkeley coordinator uses this in the collection phase.
     */
    @GetMapping("/physical")
    public ResponseEntity<?> getPhysicalTime() {
        return ResponseEntity.ok(System.currentTimeMillis());
    }

    /**
     * Endpoint to apply the logical clock offset received from the leader.
     */
    @PostMapping("/adjust")
    public ResponseEntity<?> adjustTime(@RequestParam("offset") long offset) {
        logger.info("Received leader correction: {}ms. Applying offset.", offset);
        timeSyncService.setOffset(offset);
        return ResponseEntity.ok("Logical time synchronized successfully");
    }

    /**
     * For testing/display: Get the currently calculated logical time.
     */
    @GetMapping("/logical")
    public ResponseEntity<?> getLogicalTime() {
        return ResponseEntity.ok(timeSyncService.getCurrentTime());
    }
}
