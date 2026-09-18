#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <fcitx-config/configuration.h>
#include <fcitx-config/option.h>
#include <fcitx-utils/event.h>
#include <fcitx-utils/key.h>
#include <fcitx/addoninstance.h>
#include <fcitx/event.h>
#include <fcitx/inputcontext.h>
#include <fcitx/instance.h>

// C ABI of the Rust core (src/lib.rs).
extern "C" {
struct VtConfig {
    const char *ark_api_key;
    const char *speech_api_key;
    const char *hotwords;
    const char *context;
    const char *audio_device;
};
struct VtSession;
using VtCallback = void (*)(void *user, int kind, const char *text);
VtSession *vt_session_start(const VtConfig *cfg, VtCallback cb, void *user);
void vt_session_finish(VtSession *s);
void vt_session_free(VtSession *s);
}
constexpr int VT_EVENT_PARTIAL = 0;
constexpr int VT_EVENT_FINAL = 1;
constexpr int VT_EVENT_ERROR = 2;

FCITX_CONFIGURATION(
    VoiceTypeConfig,
    fcitx::KeyListOption hotkey{
        this,
        "Hotkey",
        "Push-to-talk key (hold to speak, release to finish)",
        {fcitx::Key("Alt_R")},
        fcitx::KeyListConstrain({fcitx::KeyConstrainFlag::AllowModifierOnly,
                                 fcitx::KeyConstrainFlag::AllowModifierLess})};
    fcitx::Option<std::string> arkApiKey{
        this, "ArkApiKey", "火山方舟 Agent Plan API key (豆包 ASR 2.0)", ""};
    fcitx::Option<std::string> speechApiKey{
        this, "SpeechApiKey", "豆包语音 (speech console) API key", ""};
    fcitx::Option<std::string> hotwords{
        this, "Hotwords", "Hotwords, comma separated (names, jargon)", ""};
    fcitx::Option<std::string> context{
        this, "Context", "Free-text hint for the recognizer", ""};
    fcitx::Option<std::string> audioDevice{
        this, "AudioDevice", "Input device name substring (empty = auto)",
        ""};);

class VoiceType : public fcitx::AddonInstance {
public:
    explicit VoiceType(fcitx::Instance *instance);
    ~VoiceType() override;

    void reloadConfig() override;
    const fcitx::Configuration *getConfig() const override { return &config_; }
    void setConfig(const fcitx::RawConfig &raw) override;

private:
    // Callback context handed to the Rust core; outlives every callback.
    struct Ctx {
        VoiceType *self;
        uint64_t gen;
    };
    static void onCoreEvent(void *user, int kind, const char *text);

    void keyEvent(fcitx::KeyEvent &event);
    void start(fcitx::InputContext *ic);
    void finish();
    void stop();
    void handleEvent(uint64_t gen, int kind, const std::string &text);
    void showPreedit(const std::string &text);
    void showStatus(const std::string &text, bool autoClear);
    void clearPanel();

    fcitx::Instance *instance_;
    VoiceTypeConfig config_;
    std::vector<std::unique_ptr<fcitx::HandlerTableEntry<fcitx::EventHandler>>>
        handlers_;
    std::unique_ptr<fcitx::EventSourceTime> clearTimer_;

    VtSession *session_ = nullptr;
    std::unique_ptr<Ctx> ctx_;
    uint64_t gen_ = 0;
    bool finishing_ = false;
    fcitx::TrackableObjectReference<fcitx::InputContext> ic_;
    std::string partial_;
};
