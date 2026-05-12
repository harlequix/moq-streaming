local frame_num = 0
local function on_pts_change(name, value)
    if value == nil then return end
    frame_num = frame_num + 1
    mp.commandv("script-message", "frame-event",
        tostring(frame_num), tostring(value))
end
mp.observe_property("time-pos", "number", on_pts_change)
